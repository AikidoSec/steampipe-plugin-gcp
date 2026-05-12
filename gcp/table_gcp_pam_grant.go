package gcp

import (
	"context"
	"strings"

	"cloud.google.com/go/privilegedaccessmanager/apiv1/privilegedaccessmanagerpb"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iterator"
)

//// TABLE DEFINITION

func tableGcpPamGrant(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_pam_grant",
		Description: "GCP Privileged Access Manager Grant",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location", "entitlement_name"}),
			Hydrate:    getGcpPamGrant,
			Tags:       map[string]string{"service": "privilegedaccessmanager", "action": "grants.get"},
		},
		List: &plugin.ListConfig{
			Hydrate:       listGcpPamGrants,
			ParentHydrate: listGcpPamEntitlements,
			Tags:          map[string]string{"service": "privilegedaccessmanager", "action": "grants.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the grant resource.",
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the grant.",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "entitlement_name",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the parent entitlement.",
				Transform:   transform.FromP(pamGrantTurbotData, "EntitlementName"),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "Current state of the grant (APPROVAL_AWAITED, ACTIVE, DENIED, REVOKED, EXPIRED, ENDED, etc.).",
				Transform:   transform.FromField("State").Transform(pamGrantStateToString),
			},
			{
				Name:        "requester",
				Type:        proto.ColumnType_STRING,
				Description: "Username of the user who created this grant.",
			},
			{
				Name:        "requested_duration_seconds",
				Type:        proto.ColumnType_INT,
				Description: "The duration in seconds for which access was requested.",
				Transform:   transform.FromP(pamGrantRequestedDuration, "Seconds"),
			},
			{
				Name:        "justification",
				Type:        proto.ColumnType_STRING,
				Description: "Unstructured justification text provided by the requester.",
				Transform:   transform.From(pamGrantJustification),
			},
			{
				Name:        "externally_modified",
				Type:        proto.ColumnType_BOOL,
				Description: "Whether the policy bindings made by this grant were modified outside PAM.",
			},
			{
				Name:        "additional_email_recipients",
				Type:        proto.ColumnType_JSON,
				Description: "Additional email addresses notified for all actions on this grant.",
			},
			{
				Name:        "privileged_access",
				Type:        proto.ColumnType_JSON,
				Description: "The GCP IAM access granted by this grant.",
			},
			{
				Name:        "audit_trail",
				Type:        proto.ColumnType_JSON,
				Description: "Audit trail of access provided by this grant.",
			},
			{
				Name:        "timeline",
				Type:        proto.ColumnType_JSON,
				Description: "Timeline of state transitions for this grant.",
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the grant was created.",
				Transform:   transform.FromP(pamGrantTimestamps, "CreateTime"),
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the grant was last updated.",
				Transform:   transform.FromP(pamGrantTimestamps, "UpdateTime"),
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(pamGrantTurbotData, "Akas"),
			},

			// Standard gcp columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(pamGrantTurbotData, "Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getProject,
				Transform:   transform.FromValue(),
			},
		},
	}
}

//// LIST FUNCTION

func listGcpPamGrants(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	entitlement := h.Item.(*privilegedaccessmanagerpb.Entitlement)

	service, err := PrivilegedAccessManagerService(ctx, d)
	if err != nil {
		logger.Error("gcp_pam_grant.listGcpPamGrants", "connection_error", err)
		return nil, err
	}

	req := &privilegedaccessmanagerpb.ListGrantsRequest{
		Parent: entitlement.Name,
	}

	it := service.ListGrants(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_pam_grant.listGcpPamGrants", "api_error", err)
			return nil, err
		}

		d.StreamLeafListItem(ctx, resp)

		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}
	return nil, nil
}

//// HYDRATE FUNCTION

func getGcpPamGrant(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	location := d.EqualsQualString("location")
	entitlementName := d.EqualsQualString("entitlement_name")

	if name == "" || location == "" || entitlementName == "" {
		return nil, nil
	}

	service, err := PrivilegedAccessManagerService(ctx, d)
	if err != nil {
		logger.Error("gcp_pam_grant.getGcpPamGrant", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_pam_grant.getGcpPamGrant", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	// entitlement_name may be the short name or full resource name
	entitlementId := entitlementName
	if !strings.HasPrefix(entitlementName, "projects/") {
		entitlementId = "projects/" + project + "/locations/" + location + "/entitlements/" + entitlementName
	}

	fullName := entitlementId + "/grants/" + name

	req := &privilegedaccessmanagerpb.GetGrantRequest{
		Name: fullName,
	}

	resp, err := service.GetGrant(ctx, req)
	if err != nil {
		logger.Error("gcp_pam_grant.getGcpPamGrant", "api_error", err)
		return nil, err
	}

	return resp, nil
}

//// TRANSFORM FUNCTIONS

func pamGrantStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	state := d.Value.(privilegedaccessmanagerpb.Grant_State)
	return state.String(), nil
}

func pamGrantTimestamps(_ context.Context, d *transform.TransformData) (interface{}, error) {
	grant := d.HydrateItem.(*privilegedaccessmanagerpb.Grant)
	param := d.Param.(string)

	switch param {
	case "CreateTime":
		if grant.CreateTime == nil {
			return nil, nil
		}
		return grant.CreateTime.AsTime(), nil
	case "UpdateTime":
		if grant.UpdateTime == nil {
			return nil, nil
		}
		return grant.UpdateTime.AsTime(), nil
	}
	return nil, nil
}

func pamGrantRequestedDuration(_ context.Context, d *transform.TransformData) (interface{}, error) {
	grant := d.HydrateItem.(*privilegedaccessmanagerpb.Grant)
	if grant.RequestedDuration == nil {
		return nil, nil
	}
	return grant.RequestedDuration.Seconds, nil
}

func pamGrantJustification(_ context.Context, d *transform.TransformData) (interface{}, error) {
	grant := d.HydrateItem.(*privilegedaccessmanagerpb.Grant)
	if grant.Justification == nil {
		return nil, nil
	}
	return grant.Justification.GetUnstructuredJustification(), nil
}

func pamGrantTurbotData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	grant := d.HydrateItem.(*privilegedaccessmanagerpb.Grant)
	param := d.Param.(string)

	// Name format: projects/{project}/locations/{location}/entitlements/{entitlement-id}/grants/{grant-id}
	parts := strings.Split(grant.Name, "/")
	location := ""
	entitlementName := ""
	if len(parts) >= 4 {
		location = parts[3]
	}
	if len(parts) >= 6 {
		// projects/{p}/locations/{l}/entitlements/{e}
		entitlementName = strings.Join(parts[:6], "/")
	}

	switch param {
	case "Location":
		return location, nil
	case "EntitlementName":
		return entitlementName, nil
	case "Akas":
		return []string{"gcp://privilegedaccessmanager.googleapis.com/" + grant.Name}, nil
	}
	return nil, nil
}

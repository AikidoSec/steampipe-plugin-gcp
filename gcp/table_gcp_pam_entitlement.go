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

func tableGcpPamEntitlement(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_pam_entitlement",
		Description: "GCP Privileged Access Manager Entitlement",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location"}),
			Hydrate:    getGcpPamEntitlement,
			Tags:       map[string]string{"service": "privilegedaccessmanager", "action": "entitlements.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listGcpPamEntitlements,
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "location", Require: plugin.Optional},
			},
			Tags: map[string]string{"service": "privilegedaccessmanager", "action": "entitlements.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the entitlement resource.",
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the entitlement.",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "Current state of the entitlement (CREATING, AVAILABLE, DELETING, DELETED, UPDATING).",
				Transform:   transform.FromField("State").Transform(pamEntitlementStateToString),
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the entitlement was created.",
				Transform:   transform.FromP(pamEntitlementTimestamps, "CreateTime"),
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the entitlement was last updated.",
				Transform:   transform.FromP(pamEntitlementTimestamps, "UpdateTime"),
			},
			{
				Name:        "max_request_duration_seconds",
				Type:        proto.ColumnType_INT,
				Description: "Maximum duration in seconds that access can be requested for.",
				Transform:   transform.FromP(pamEntitlementMaxDuration, "Seconds"),
			},
			{
				Name:        "etag",
				Type:        proto.ColumnType_STRING,
				Description: "Optimistic concurrency control token.",
			},
			{
				Name:        "eligible_users",
				Type:        proto.ColumnType_JSON,
				Description: "Principals who can create grants using this entitlement.",
			},
			{
				Name:        "approval_workflow",
				Type:        proto.ColumnType_JSON,
				Description: "Approval workflow configuration for grants under this entitlement.",
			},
			{
				Name:        "privileged_access",
				Type:        proto.ColumnType_JSON,
				Description: "The GCP IAM access configuration granted on approval.",
			},
			{
				Name:        "requester_justification_config",
				Type:        proto.ColumnType_JSON,
				Description: "Configuration for the justification required from the requester.",
			},
			{
				Name:        "additional_notification_targets",
				Type:        proto.ColumnType_JSON,
				Description: "Additional email addresses to be notified for grant actions.",
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
				Transform:   transform.FromP(pamEntitlementTurbotData, "Akas"),
			},

			// Standard gcp columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(pamEntitlementTurbotData, "Location"),
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

func listGcpPamEntitlements(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	service, err := PrivilegedAccessManagerService(ctx, d)
	if err != nil {
		logger.Error("gcp_pam_entitlement.listGcpPamEntitlements", "connection_error", err)
		return nil, err
	}

	location := d.EqualsQualString("location")
	if location == "" {
		location = "-"
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_pam_entitlement.listGcpPamEntitlements", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	parent := "projects/" + project + "/locations/" + location
	req := &privilegedaccessmanagerpb.ListEntitlementsRequest{
		Parent: parent,
	}

	it := service.ListEntitlements(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_pam_entitlement.listGcpPamEntitlements", "api_error", err)
			return nil, err
		}

		d.StreamListItem(ctx, resp)

		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}
	return nil, nil
}

//// HYDRATE FUNCTION

func getGcpPamEntitlement(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	location := d.EqualsQualString("location")

	if name == "" || location == "" {
		return nil, nil
	}

	service, err := PrivilegedAccessManagerService(ctx, d)
	if err != nil {
		logger.Error("gcp_pam_entitlement.getGcpPamEntitlement", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_pam_entitlement.getGcpPamEntitlement", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	fullName := "projects/" + project + "/locations/" + location + "/entitlements/" + name

	req := &privilegedaccessmanagerpb.GetEntitlementRequest{
		Name: fullName,
	}

	resp, err := service.GetEntitlement(ctx, req)
	if err != nil {
		logger.Error("gcp_pam_entitlement.getGcpPamEntitlement", "api_error", err)
		return nil, err
	}

	return resp, nil
}

//// TRANSFORM FUNCTIONS

func pamEntitlementStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	state := d.Value.(privilegedaccessmanagerpb.Entitlement_State)
	return state.String(), nil
}

func pamEntitlementTimestamps(_ context.Context, d *transform.TransformData) (interface{}, error) {
	entitlement := d.HydrateItem.(*privilegedaccessmanagerpb.Entitlement)
	param := d.Param.(string)

	switch param {
	case "CreateTime":
		if entitlement.CreateTime == nil {
			return nil, nil
		}
		return entitlement.CreateTime.AsTime(), nil
	case "UpdateTime":
		if entitlement.UpdateTime == nil {
			return nil, nil
		}
		return entitlement.UpdateTime.AsTime(), nil
	}
	return nil, nil
}

func pamEntitlementMaxDuration(_ context.Context, d *transform.TransformData) (interface{}, error) {
	entitlement := d.HydrateItem.(*privilegedaccessmanagerpb.Entitlement)
	if entitlement.MaxRequestDuration == nil {
		return nil, nil
	}
	return entitlement.MaxRequestDuration.Seconds, nil
}

func pamEntitlementTurbotData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	entitlement := d.HydrateItem.(*privilegedaccessmanagerpb.Entitlement)
	param := d.Param.(string)

	// Name format: projects/{project}/locations/{location}/entitlements/{id}
	parts := strings.Split(entitlement.Name, "/")
	location := ""
	if len(parts) >= 4 {
		location = parts[3]
	}

	switch param {
	case "Location":
		return location, nil
	case "Akas":
		return []string{"gcp://privilegedaccessmanager.googleapis.com/" + entitlement.Name}, nil
	}
	return nil, nil
}

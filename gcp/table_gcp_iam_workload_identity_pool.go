package gcp

import (
	"context"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iam/v1"
)

//// TABLE DEFINITION

func tableGcpIamWorkloadIdentityPool(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_iam_workload_identity_pool",
		Description: "GCP IAM Workload Identity Pool",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location"}),
			Hydrate:    getGcpIamWorkloadIdentityPool,
			Tags:       map[string]string{"service": "iam", "action": "workloadIdentityPools.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listGcpIamWorkloadIdentityPools,
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "location", Require: plugin.Optional},
			},
			Tags: map[string]string{"service": "iam", "action": "workloadIdentityPools.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The short name of the workload identity pool.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Description: "Full resource name of the workload identity pool.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "display_name",
				Description: "A display name for the pool.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "A description of the pool.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "state",
				Description: "The state of the pool. Possible values are: ACTIVE, DELETED.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "disabled",
				Description: "Whether the pool is disabled.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "mode",
				Description: "The mode the pool is operating in. Possible values are: FEDERATION_ONLY, TRUST_DOMAIN, SYSTEM_TRUST_DOMAIN.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "expire_time",
				Description: "Time after which the workload identity pool will be permanently purged and cannot be recovered.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "inline_certificate_issuance_config",
				Description: "Defines the Certificate Authority pool resources and configurations required for issuance and rotation of mTLS workload certificates.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "inline_trust_config",
				Description: "Represents config to add additional trusted trust domains.",
				Type:        proto.ColumnType_JSON,
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DisplayName").Transform(transform.NullIfZeroValue),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Name").Transform(wifPoolAkas),
			},

			// GCP standard columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(wifPoolLocation),
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

func listGcpIamWorkloadIdentityPools(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listGcpIamWorkloadIdentityPools")

	service, err := IAMService(ctx, d)
	if err != nil {
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	location := d.EqualsQualString("location")
	if location == "" {
		location = "global"
	}

	pageSize := types.Int64(1000)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	parent := "projects/" + project + "/locations/" + location
	resp := service.Projects.Locations.WorkloadIdentityPools.List(parent).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *iam.ListWorkloadIdentityPoolsResponse) error {
		d.WaitForListRateLimit(ctx)

		for _, pool := range page.WorkloadIdentityPools {
			d.StreamListItem(ctx, pool)

			if d.RowsRemaining(ctx) == 0 {
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getGcpIamWorkloadIdentityPool(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("getGcpIamWorkloadIdentityPool")

	name := d.EqualsQualString("name")
	location := d.EqualsQualString("location")

	if name == "" {
		return nil, nil
	}
	if location == "" {
		return nil, nil
	}

	service, err := IAMService(ctx, d)
	if err != nil {
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	fullName := "projects/" + project + "/locations/" + location + "/workloadIdentityPools/" + name

	resp, err := service.Projects.Locations.WorkloadIdentityPools.Get(fullName).Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

//// TRANSFORM FUNCTIONS

// name format: projects/{project}/locations/{location}/workloadIdentityPools/{pool_id}

func wifPoolLocation(_ context.Context, d *transform.TransformData) (interface{}, error) {
	name := types.SafeString(d.Value)
	parts := strings.Split(name, "/")
	if len(parts) >= 4 {
		return parts[3], nil
	}
	return nil, nil
}

func wifPoolAkas(_ context.Context, d *transform.TransformData) (interface{}, error) {
	name := types.SafeString(d.Value)
	return []string{"gcp://iam.googleapis.com/" + name}, nil
}

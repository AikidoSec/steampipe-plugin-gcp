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

func tableGcpIamWorkloadIdentityPoolProvider(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_iam_workload_identity_pool_provider",
		Description: "GCP IAM Workload Identity Pool Provider",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "pool_name", "location"}),
			Hydrate:    getGcpIamWorkloadIdentityPoolProvider,
			Tags:       map[string]string{"service": "iam", "action": "workloadIdentityPools.providers.get"},
		},
		List: &plugin.ListConfig{
			ParentHydrate: listGcpIamWorkloadIdentityPools,
			Hydrate:       listGcpIamWorkloadIdentityPoolProviders,
			Tags:          map[string]string{"service": "iam", "action": "workloadIdentityPools.providers.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The short name of the workload identity pool provider.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "pool_name",
				Description: "The short name of the parent workload identity pool.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(wifProviderPoolName),
			},
			{
				Name:        "self_link",
				Description: "Full resource name of the workload identity pool provider.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "display_name",
				Description: "A display name for the provider.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "A description for the provider.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "state",
				Description: "The state of the provider. Possible values are: ACTIVE, DELETED.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "disabled",
				Description: "Whether the provider is disabled.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "expire_time",
				Description: "Time after which the provider will be permanently purged and cannot be recovered.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "attribute_condition",
				Description: "A Common Expression Language expression to restrict valid authentication credentials.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "attribute_mapping",
				Description: "Maps attributes from authentication credentials to Google Cloud attributes.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "aws",
				Description: "Amazon Web Services identity provider configuration.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "oidc",
				Description: "OpenId Connect 1.0 identity provider configuration.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "saml",
				Description: "SAML 2.0 identity provider configuration.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "x509",
				Description: "X.509-type identity provider configuration.",
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
				Transform:   transform.FromField("Name").Transform(wifProviderAkas),
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

func listGcpIamWorkloadIdentityPoolProviders(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listGcpIamWorkloadIdentityPoolProviders")

	pool := h.Item.(*iam.WorkloadIdentityPool)

	service, err := IAMService(ctx, d)
	if err != nil {
		return nil, err
	}

	pageSize := types.Int64(1000)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	resp := service.Projects.Locations.WorkloadIdentityPools.Providers.List(pool.Name).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *iam.ListWorkloadIdentityPoolProvidersResponse) error {
		d.WaitForListRateLimit(ctx)

		for _, provider := range page.WorkloadIdentityPoolProviders {
			d.StreamListItem(ctx, provider)

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

func getGcpIamWorkloadIdentityPoolProvider(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("getGcpIamWorkloadIdentityPoolProvider")

	name := d.EqualsQualString("name")
	poolName := d.EqualsQualString("pool_name")
	location := d.EqualsQualString("location")

	if name == "" {
		return nil, nil
	}
	if poolName == "" {
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

	fullName := "projects/" + project + "/locations/" + location + "/workloadIdentityPools/" + poolName + "/providers/" + name

	resp, err := service.Projects.Locations.WorkloadIdentityPools.Providers.Get(fullName).Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

//// TRANSFORM FUNCTIONS

// name format: projects/{project}/locations/{location}/workloadIdentityPools/{pool_id}/providers/{provider_id}

func wifProviderPoolName(_ context.Context, d *transform.TransformData) (interface{}, error) {
	name := types.SafeString(d.Value)
	parts := strings.Split(name, "/")
	if len(parts) >= 6 {
		return parts[5], nil
	}
	return nil, nil
}

func wifProviderAkas(_ context.Context, d *transform.TransformData) (interface{}, error) {
	name := types.SafeString(d.Value)
	return []string{"gcp://iam.googleapis.com/" + name}, nil
}

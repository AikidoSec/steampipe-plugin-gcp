package gcp

import (
	"context"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iterator"
)

//// TABLE DEFINITION

func tableGcpSpannerInstanceConfig(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_spanner_instance_config",
		Description: "GCP Spanner Instance Configuration",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getGcpSpannerInstanceConfig,
			Tags:       map[string]string{"service": "spanner", "action": "instanceConfigs.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listGcpSpannerInstanceConfigs,
			Tags:    map[string]string{"service": "spanner", "action": "instanceConfigs.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the instance configuration.",
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the instance configuration.",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The display name for this instance configuration.",
			},
			{
				Name:        "config_type",
				Type:        proto.ColumnType_STRING,
				Description: "Whether this is a Google-managed or user-managed configuration (GOOGLE_MANAGED, USER_MANAGED).",
				Transform:   transform.FromField("ConfigType").Transform(spannerInstanceConfigTypeToString),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current state of the instance configuration (CREATING, READY, etc.). Only set for user-managed configurations.",
				Transform:   transform.FromField("State").Transform(spannerInstanceConfigStateToString),
			},
			{
				Name:        "base_config",
				Type:        proto.ColumnType_STRING,
				Description: "The base Google-managed configuration this user-managed configuration is derived from.",
			},
			{
				Name:        "reconciling",
				Type:        proto.ColumnType_BOOL,
				Description: "Whether the instance configuration is currently being created or updated.",
			},
			{
				Name:        "etag",
				Type:        proto.ColumnType_STRING,
				Description: "Optimistic concurrency control token.",
			},
			{
				Name:        "leader_options",
				Type:        proto.ColumnType_JSON,
				Description: "Allowed values for the default_leader schema option for databases in instances using this configuration.",
			},
			{
				Name:        "replicas",
				Type:        proto.ColumnType_JSON,
				Description: "The geographic placement of nodes and their replication properties.",
			},
			{
				Name:        "optional_replicas",
				Type:        proto.ColumnType_JSON,
				Description: "The available optional replicas for user-managed configurations.",
			},
			{
				Name:        "free_instance_availability",
				Type:        proto.ColumnType_STRING,
				Description: "Whether free instances are available to be created in this configuration.",
				Transform:   transform.FromField("FreeInstanceAvailability").Transform(spannerFreeInstanceAvailabilityToString),
			},
			{
				Name:        "quorum_type",
				Type:        proto.ColumnType_STRING,
				Description: "The quorum type of the instance configuration.",
				Transform:   transform.FromField("QuorumType").Transform(spannerQuorumTypeToString),
			},
			{
				Name:        "storage_limit_per_processing_unit",
				Type:        proto.ColumnType_INT,
				Description: "The storage limit in bytes per processing unit.",
			},
			{
				Name:        "labels",
				Type:        proto.ColumnType_JSON,
				Description: "Cloud labels associated with this resource.",
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DisplayName"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Name").Transform(spannerSelfLinkToAkas),
			},

			// Standard gcp columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
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

func listGcpSpannerInstanceConfigs(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_instance_config.listGcpSpannerInstanceConfigs", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_instance_config.listGcpSpannerInstanceConfigs", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	req := &instancepb.ListInstanceConfigsRequest{
		Parent: "projects/" + project,
	}

	it := service.ListInstanceConfigs(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_spanner_instance_config.listGcpSpannerInstanceConfigs", "api_error", err)
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

func getGcpSpannerInstanceConfig(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	if name == "" {
		return nil, nil
	}

	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_instance_config.getGcpSpannerInstanceConfig", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_instance_config.getGcpSpannerInstanceConfig", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	req := &instancepb.GetInstanceConfigRequest{
		Name: "projects/" + project + "/instanceConfigs/" + name,
	}

	resp, err := service.GetInstanceConfig(ctx, req)
	if err != nil {
		logger.Error("gcp_spanner_instance_config.getGcpSpannerInstanceConfig", "api_error", err)
		return nil, err
	}

	return resp, nil
}

//// TRANSFORM FUNCTIONS

func spannerInstanceConfigTypeToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.InstanceConfig_Type(d.Value.(instancepb.InstanceConfig_Type)).String(), nil
}

func spannerInstanceConfigStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.InstanceConfig_State(d.Value.(instancepb.InstanceConfig_State)).String(), nil
}

func spannerFreeInstanceAvailabilityToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.InstanceConfig_FreeInstanceAvailability(d.Value.(instancepb.InstanceConfig_FreeInstanceAvailability)).String(), nil
}

func spannerQuorumTypeToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.InstanceConfig_QuorumType(d.Value.(instancepb.InstanceConfig_QuorumType)).String(), nil
}

func spannerSelfLinkToAkas(_ context.Context, d *transform.TransformData) (interface{}, error) {
	name := d.Value.(string)
	return []string{"gcp://spanner.googleapis.com/" + name}, nil
}

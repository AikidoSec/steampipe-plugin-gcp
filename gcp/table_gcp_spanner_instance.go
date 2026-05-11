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

func tableGcpSpannerInstance(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_spanner_instance",
		Description: "GCP Spanner Instance",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getGcpSpannerInstance,
			Tags:       map[string]string{"service": "spanner", "action": "instances.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listGcpSpannerInstances,
			Tags:    map[string]string{"service": "spanner", "action": "instances.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the instance.",
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the instance.",
				Transform:   transform.FromField("Name"),
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The descriptive name for this instance as it appears in UIs.",
			},
			{
				Name:        "config",
				Type:        proto.ColumnType_STRING,
				Description: "The full resource name of the instance configuration.",
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current instance state (CREATING, READY).",
				Transform:   transform.FromField("State").Transform(spannerInstanceStateToString),
			},
			{
				Name:        "instance_type",
				Type:        proto.ColumnType_STRING,
				Description: "The type of the instance (READ_WRITE, FREE_INSTANCE, READ_ONLY, PROVISIONED).",
				Transform:   transform.FromField("InstanceType").Transform(spannerInstanceTypeToString),
			},
			{
				Name:        "edition",
				Type:        proto.ColumnType_STRING,
				Description: "The edition of the instance (STANDARD, ENTERPRISE, ENTERPRISE_PLUS).",
				Transform:   transform.FromField("Edition").Transform(spannerInstanceEditionToString),
			},
			{
				Name:        "node_count",
				Type:        proto.ColumnType_INT,
				Description: "The number of nodes allocated to this instance.",
			},
			{
				Name:        "processing_units",
				Type:        proto.ColumnType_INT,
				Description: "The number of processing units allocated to this instance.",
			},
			{
				Name:        "default_backup_schedule_type",
				Type:        proto.ColumnType_STRING,
				Description: "The default backup schedule behavior for new databases (AUTOMATIC, NONE).",
				Transform:   transform.FromField("DefaultBackupScheduleType").Transform(spannerDefaultBackupScheduleTypeToString),
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which the instance was created.",
				Transform:   transform.FromP(spannerInstanceTimestamps, "CreateTime"),
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which the instance was most recently updated.",
				Transform:   transform.FromP(spannerInstanceTimestamps, "UpdateTime"),
			},
			{
				Name:        "autoscaling_config",
				Type:        proto.ColumnType_JSON,
				Description: "Autoscaling configuration. Autoscaling is enabled if this field is set.",
			},
			{
				Name:        "replica_compute_capacity",
				Type:        proto.ColumnType_JSON,
				Description: "Compute capacity per replica selection.",
			},
			{
				Name:        "free_instance_metadata",
				Type:        proto.ColumnType_JSON,
				Description: "Free instance metadata. Only populated for free instances.",
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
				Transform:   transform.FromP(spannerInstanceLocation, "Location"),
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

func listGcpSpannerInstances(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_instance.listGcpSpannerInstances", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_instance.listGcpSpannerInstances", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	req := &instancepb.ListInstancesRequest{
		Parent: "projects/" + project,
	}

	it := service.ListInstances(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_spanner_instance.listGcpSpannerInstances", "api_error", err)
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

func getGcpSpannerInstance(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	if name == "" {
		return nil, nil
	}

	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_instance.getGcpSpannerInstance", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_instance.getGcpSpannerInstance", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	req := &instancepb.GetInstanceRequest{
		Name: "projects/" + project + "/instances/" + name,
	}

	resp, err := service.GetInstance(ctx, req)
	if err != nil {
		logger.Error("gcp_spanner_instance.getGcpSpannerInstance", "api_error", err)
		return nil, err
	}

	return resp, nil
}

//// TRANSFORM FUNCTIONS

func spannerInstanceStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.Instance_State(d.Value.(instancepb.Instance_State)).String(), nil
}

func spannerInstanceTypeToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.Instance_InstanceType(d.Value.(instancepb.Instance_InstanceType)).String(), nil
}

func spannerInstanceEditionToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.Instance_Edition(d.Value.(instancepb.Instance_Edition)).String(), nil
}

func spannerDefaultBackupScheduleTypeToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.Instance_DefaultBackupScheduleType(d.Value.(instancepb.Instance_DefaultBackupScheduleType)).String(), nil
}

func spannerInstanceTimestamps(_ context.Context, d *transform.TransformData) (interface{}, error) {
	instance := d.HydrateItem.(*instancepb.Instance)
	param := d.Param.(string)

	switch param {
	case "CreateTime":
		if instance.CreateTime == nil {
			return nil, nil
		}
		return instance.CreateTime.AsTime(), nil
	case "UpdateTime":
		if instance.UpdateTime == nil {
			return nil, nil
		}
		return instance.UpdateTime.AsTime(), nil
	}
	return nil, nil
}

func spannerInstanceLocation(_ context.Context, d *transform.TransformData) (interface{}, error) {
	instance := d.HydrateItem.(*instancepb.Instance)
	// Config is "projects/{project}/instanceConfigs/{config-id}"
	// The config-id encodes the region (e.g. "us-central1", "nam3", "regional-us-east1")
	return getLastPathElement(instance.Config), nil
}

// fetchSpannerInstanceLocation retrieves the location for a given instance resource name.
// Used by child resource Get functions that need to populate InstanceLocation.
func fetchSpannerInstanceLocation(ctx context.Context, d *plugin.QueryData, instanceResourceName string) (string, error) {
	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		return "", err
	}

	instance, err := service.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: instanceResourceName})
	if err != nil {
		return "", err
	}

	return getLastPathElement(instance.Config), nil
}

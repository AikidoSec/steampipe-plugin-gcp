package gcp

import (
	"context"
	"strings"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iterator"
)

type spannerInstancePartitionRow struct {
	*instancepb.InstancePartition
	InstanceLocation string
}

//// TABLE DEFINITION

func tableGcpSpannerInstancePartition(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_spanner_instance_partition",
		Description: "GCP Spanner Instance Partition",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "instance_name"}),
			Hydrate:    getGcpSpannerInstancePartition,
			Tags:       map[string]string{"service": "spanner", "action": "instancePartitions.get"},
		},
		List: &plugin.ListConfig{
			Hydrate:       listGcpSpannerInstancePartitions,
			ParentHydrate: listGcpSpannerInstances,
			Tags:          map[string]string{"service": "spanner", "action": "instancePartitions.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the instance partition.",
				Transform:   transform.FromField("InstancePartition.Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the instance partition.",
				Transform:   transform.FromField("InstancePartition.Name"),
			},
			{
				Name:        "instance_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the instance this partition belongs to.",
				Transform:   transform.FromP(spannerInstancePartitionTurbotData, "InstanceName"),
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "The descriptive name for this instance partition as it appears in UIs.",
				Transform:   transform.FromField("InstancePartition.DisplayName"),
			},
			{
				Name:        "config",
				Type:        proto.ColumnType_STRING,
				Description: "The full resource name of the instance partition's configuration.",
				Transform:   transform.FromField("InstancePartition.Config"),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current instance partition state (CREATING, READY).",
				Transform:   transform.FromField("InstancePartition.State").Transform(spannerInstancePartitionStateToString),
			},
			{
				Name:        "node_count",
				Type:        proto.ColumnType_INT,
				Description: "The number of nodes allocated to this instance partition.",
				Transform:   transform.FromP(spannerInstancePartitionComputeCapacity, "NodeCount"),
			},
			{
				Name:        "processing_units",
				Type:        proto.ColumnType_INT,
				Description: "The number of processing units allocated to this instance partition.",
				Transform:   transform.FromP(spannerInstancePartitionComputeCapacity, "ProcessingUnits"),
			},
			{
				Name:        "etag",
				Type:        proto.ColumnType_STRING,
				Description: "Optimistic concurrency control token.",
				Transform:   transform.FromField("InstancePartition.Etag"),
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which the instance partition was created.",
				Transform:   transform.FromP(spannerInstancePartitionTimestamps, "CreateTime"),
			},
			{
				Name:        "update_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which the instance partition was most recently updated.",
				Transform:   transform.FromP(spannerInstancePartitionTimestamps, "UpdateTime"),
			},
			{
				Name:        "autoscaling_config",
				Type:        proto.ColumnType_JSON,
				Description: "Autoscaling configuration. Autoscaling is enabled if this field is set.",
				Transform:   transform.FromField("InstancePartition.AutoscalingConfig"),
			},
			{
				Name:        "referencing_databases",
				Type:        proto.ColumnType_JSON,
				Description: "Names of databases that reference this instance partition.",
				Transform:   transform.FromField("InstancePartition.ReferencingDatabases"),
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("InstancePartition.DisplayName"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("InstancePartition.Name").Transform(spannerSelfLinkToAkas),
			},

			// Standard gcp columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("InstanceLocation"),
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

func listGcpSpannerInstancePartitions(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	instance := h.Item.(*instancepb.Instance)
	location := getLastPathElement(instance.Config)

	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_instance_partition.listGcpSpannerInstancePartitions", "connection_error", err)
		return nil, err
	}

	req := &instancepb.ListInstancePartitionsRequest{
		Parent: instance.Name,
	}

	it := service.ListInstancePartitions(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_spanner_instance_partition.listGcpSpannerInstancePartitions", "api_error", err)
			return nil, err
		}

		d.StreamLeafListItem(ctx, &spannerInstancePartitionRow{InstancePartition: resp, InstanceLocation: location})

		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}
	return nil, nil
}

//// HYDRATE FUNCTION

func getGcpSpannerInstancePartition(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	instanceName := d.EqualsQualString("instance_name")

	if name == "" || instanceName == "" {
		return nil, nil
	}

	service, err := SpannerInstanceAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_instance_partition.getGcpSpannerInstancePartition", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_instance_partition.getGcpSpannerInstancePartition", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	instanceId := instanceName
	if !strings.HasPrefix(instanceName, "projects/") {
		instanceId = "projects/" + project + "/instances/" + instanceName
	}

	req := &instancepb.GetInstancePartitionRequest{
		Name: instanceId + "/instancePartitions/" + name,
	}

	resp, err := service.GetInstancePartition(ctx, req)
	if err != nil {
		logger.Error("gcp_spanner_instance_partition.getGcpSpannerInstancePartition", "api_error", err)
		return nil, err
	}

	return &spannerInstancePartitionRow{InstancePartition: resp, InstanceLocation: ""}, nil
}

//// TRANSFORM FUNCTIONS

func spannerInstancePartitionStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return instancepb.InstancePartition_State(d.Value.(instancepb.InstancePartition_State)).String(), nil
}

func spannerInstancePartitionTimestamps(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerInstancePartitionRow)
	param := d.Param.(string)

	switch param {
	case "CreateTime":
		if row.InstancePartition.CreateTime == nil {
			return nil, nil
		}
		return row.InstancePartition.CreateTime.AsTime(), nil
	case "UpdateTime":
		if row.InstancePartition.UpdateTime == nil {
			return nil, nil
		}
		return row.InstancePartition.UpdateTime.AsTime(), nil
	}
	return nil, nil
}

func spannerInstancePartitionComputeCapacity(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerInstancePartitionRow)
	param := d.Param.(string)

	switch param {
	case "NodeCount":
		return row.InstancePartition.GetNodeCount(), nil
	case "ProcessingUnits":
		return row.InstancePartition.GetProcessingUnits(), nil
	}
	return nil, nil
}

func spannerInstancePartitionTurbotData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerInstancePartitionRow)
	param := d.Param.(string)

	// Name format: projects/{project}/instances/{instance}/instancePartitions/{partition}
	parts := strings.Split(row.InstancePartition.Name, "/")

	switch param {
	case "InstanceName":
		if len(parts) >= 4 {
			return parts[3], nil
		}
	}
	return nil, nil
}

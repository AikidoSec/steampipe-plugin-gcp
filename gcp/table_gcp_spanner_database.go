package gcp

import (
	"context"
	"strings"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/iterator"
)

type spannerDatabaseRow struct {
	*databasepb.Database
	InstanceLocation string
}

//// TABLE DEFINITION

func tableGcpSpannerDatabase(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_spanner_database",
		Description: "GCP Spanner Database",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "instance_name"}),
			Hydrate:    getGcpSpannerDatabase,
			Tags:       map[string]string{"service": "spanner", "action": "databases.get"},
		},
		List: &plugin.ListConfig{
			Hydrate:       listGcpSpannerDatabases,
			ParentHydrate: listGcpSpannerInstances,
			Tags:          map[string]string{"service": "spanner", "action": "databases.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the database.",
				Transform:   transform.FromField("Database.Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the database.",
				Transform:   transform.FromField("Database.Name"),
			},
			{
				Name:        "instance_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the instance this database belongs to.",
				Transform:   transform.FromP(spannerDatabaseTurbotData, "InstanceName"),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current database state (CREATING, READY, READY_OPTIMIZING).",
				Transform:   transform.FromField("Database.State").Transform(spannerDatabaseStateToString),
			},
			{
				Name:        "database_dialect",
				Type:        proto.ColumnType_STRING,
				Description: "The dialect of the database (GOOGLE_STANDARD_SQL, POSTGRESQL).",
				Transform:   transform.FromField("Database.DatabaseDialect").Transform(spannerDatabaseDialectToString),
			},
			{
				Name:        "default_leader",
				Type:        proto.ColumnType_STRING,
				Description: "The read-write region which contains the database's leader replicas.",
				Transform:   transform.FromField("Database.DefaultLeader"),
			},
			{
				Name:        "version_retention_period",
				Type:        proto.ColumnType_STRING,
				Description: "The period in which Cloud Spanner retains all versions of data for the database.",
				Transform:   transform.FromField("Database.VersionRetentionPeriod"),
			},
			{
				Name:        "enable_drop_protection",
				Type:        proto.ColumnType_BOOL,
				Description: "Whether drop protection is enabled for this database.",
				Transform:   transform.FromField("Database.EnableDropProtection"),
			},
			{
				Name:        "reconciling",
				Type:        proto.ColumnType_BOOL,
				Description: "Whether the database is currently being updated.",
				Transform:   transform.FromField("Database.Reconciling"),
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time at which the database creation started.",
				Transform:   transform.FromP(spannerDatabaseTimestamps, "CreateTime"),
			},
			{
				Name:        "earliest_version_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "Earliest timestamp at which older versions of the data can be read.",
				Transform:   transform.FromP(spannerDatabaseTimestamps, "EarliestVersionTime"),
			},
			{
				Name:        "encryption_config",
				Type:        proto.ColumnType_JSON,
				Description: "Customer managed encryption configuration for the database.",
				Transform:   transform.FromField("Database.EncryptionConfig"),
			},
			{
				Name:        "encryption_info",
				Type:        proto.ColumnType_JSON,
				Description: "Encryption information for the database, including all KMS key versions in use.",
				Transform:   transform.FromField("Database.EncryptionInfo"),
			},
			{
				Name:        "restore_info",
				Type:        proto.ColumnType_JSON,
				Description: "Information about the restore source. Only set for restored databases.",
				Transform:   transform.FromField("Database.RestoreInfo"),
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Database.Name").Transform(lastPathElement),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Database.Name").Transform(spannerSelfLinkToAkas),
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

func listGcpSpannerDatabases(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	instance := h.Item.(*instancepb.Instance)
	location := getLastPathElement(instance.Config)

	service, err := SpannerDatabaseAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_database.listGcpSpannerDatabases", "connection_error", err)
		return nil, err
	}

	req := &databasepb.ListDatabasesRequest{
		Parent: instance.Name,
	}

	it := service.ListDatabases(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_spanner_database.listGcpSpannerDatabases", "api_error", err)
			return nil, err
		}

		d.StreamLeafListItem(ctx, &spannerDatabaseRow{Database: resp, InstanceLocation: location})

		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}
	return nil, nil
}

//// HYDRATE FUNCTION

func getGcpSpannerDatabase(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	instanceName := d.EqualsQualString("instance_name")

	if name == "" || instanceName == "" {
		return nil, nil
	}

	service, err := SpannerDatabaseAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_database.getGcpSpannerDatabase", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_database.getGcpSpannerDatabase", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	instanceId := instanceName
	if !strings.HasPrefix(instanceName, "projects/") {
		instanceId = "projects/" + project + "/instances/" + instanceName
	}

	req := &databasepb.GetDatabaseRequest{
		Name: instanceId + "/databases/" + name,
	}

	resp, err := service.GetDatabase(ctx, req)
	if err != nil {
		logger.Error("gcp_spanner_database.getGcpSpannerDatabase", "api_error", err)
		return nil, err
	}

	return &spannerDatabaseRow{Database: resp, InstanceLocation: ""}, nil
}

//// TRANSFORM FUNCTIONS

func spannerDatabaseStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return databasepb.Database_State(d.Value.(databasepb.Database_State)).String(), nil
}

func spannerDatabaseDialectToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return databasepb.DatabaseDialect(d.Value.(databasepb.DatabaseDialect)).String(), nil
}

func spannerDatabaseTimestamps(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerDatabaseRow)
	param := d.Param.(string)

	switch param {
	case "CreateTime":
		if row.Database.CreateTime == nil {
			return nil, nil
		}
		return row.Database.CreateTime.AsTime(), nil
	case "EarliestVersionTime":
		if row.Database.EarliestVersionTime == nil {
			return nil, nil
		}
		return row.Database.EarliestVersionTime.AsTime(), nil
	}
	return nil, nil
}

func spannerDatabaseTurbotData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerDatabaseRow)
	param := d.Param.(string)

	// Name format: projects/{project}/instances/{instance}/databases/{db}
	parts := strings.Split(row.Database.Name, "/")

	switch param {
	case "InstanceName":
		if len(parts) >= 4 {
			return parts[3], nil
		}
	}
	return nil, nil
}

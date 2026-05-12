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

type spannerBackupRow struct {
	*databasepb.Backup
	InstanceLocation string
}

//// TABLE DEFINITION

func tableGcpSpannerBackup(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_spanner_backup",
		Description: "GCP Spanner Backup",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "instance_name"}),
			Hydrate:    getGcpSpannerBackup,
			Tags:       map[string]string{"service": "spanner", "action": "backups.get"},
		},
		List: &plugin.ListConfig{
			Hydrate:       listGcpSpannerBackups,
			ParentHydrate: listGcpSpannerInstances,
			Tags:          map[string]string{"service": "spanner", "action": "backups.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the backup.",
				Transform:   transform.FromField("Backup.Name").Transform(lastPathElement),
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Full resource name of the backup.",
				Transform:   transform.FromField("Backup.Name"),
			},
			{
				Name:        "instance_name",
				Type:        proto.ColumnType_STRING,
				Description: "The name of the instance this backup belongs to.",
				Transform:   transform.FromP(spannerBackupTurbotData, "InstanceName"),
			},
			{
				Name:        "database",
				Type:        proto.ColumnType_STRING,
				Description: "The full resource name of the source database.",
				Transform:   transform.FromField("Backup.Database"),
			},
			{
				Name:        "state",
				Type:        proto.ColumnType_STRING,
				Description: "The current backup state (CREATING, READY).",
				Transform:   transform.FromField("Backup.State").Transform(spannerBackupStateToString),
			},
			{
				Name:        "database_dialect",
				Type:        proto.ColumnType_STRING,
				Description: "The dialect of the backed-up database.",
				Transform:   transform.FromField("Backup.DatabaseDialect").Transform(spannerDatabaseDialectToString),
			},
			{
				Name:        "size_bytes",
				Type:        proto.ColumnType_INT,
				Description: "Size of the backup in bytes.",
				Transform:   transform.FromField("Backup.SizeBytes"),
			},
			{
				Name:        "freeable_size_bytes",
				Type:        proto.ColumnType_INT,
				Description: "The number of bytes that will be freed by deleting this backup.",
				Transform:   transform.FromField("Backup.FreeableSizeBytes"),
			},
			{
				Name:        "exclusive_size_bytes",
				Type:        proto.ColumnType_INT,
				Description: "Storage space needed to keep the data that has changed since the previous backup.",
				Transform:   transform.FromField("Backup.ExclusiveSizeBytes"),
			},
			{
				Name:        "incremental_backup_chain_id",
				Type:        proto.ColumnType_STRING,
				Description: "Chain ID shared by all backups in the same incremental backup chain.",
				Transform:   transform.FromField("Backup.IncrementalBackupChainId"),
			},
			{
				Name:        "create_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The time the CreateBackup request was received.",
				Transform:   transform.FromP(spannerBackupTimestamps, "CreateTime"),
			},
			{
				Name:        "version_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The timestamp at which the backup was taken (externally consistent copy).",
				Transform:   transform.FromP(spannerBackupTimestamps, "VersionTime"),
			},
			{
				Name:        "expire_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The expiration time of the backup.",
				Transform:   transform.FromP(spannerBackupTimestamps, "ExpireTime"),
			},
			{
				Name:        "max_expire_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The maximum allowed expiration time for this backup.",
				Transform:   transform.FromP(spannerBackupTimestamps, "MaxExpireTime"),
			},
			{
				Name:        "oldest_version_time",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "The oldest version time guaranteed to be retained to support this backup.",
				Transform:   transform.FromP(spannerBackupTimestamps, "OldestVersionTime"),
			},
			{
				Name:        "referencing_databases",
				Type:        proto.ColumnType_JSON,
				Description: "Names of databases that have been restored from this backup.",
				Transform:   transform.FromField("Backup.ReferencingDatabases"),
			},
			{
				Name:        "referencing_backups",
				Type:        proto.ColumnType_JSON,
				Description: "Names of destination backups being created by copying this source backup.",
				Transform:   transform.FromField("Backup.ReferencingBackups"),
			},
			{
				Name:        "backup_schedules",
				Type:        proto.ColumnType_JSON,
				Description: "Backup schedule URIs associated with creating this backup.",
				Transform:   transform.FromField("Backup.BackupSchedules"),
			},
			{
				Name:        "instance_partitions",
				Type:        proto.ColumnType_JSON,
				Description: "Instance partitions storing this backup.",
				Transform:   transform.FromField("Backup.InstancePartitions"),
			},
			{
				Name:        "encryption_info",
				Type:        proto.ColumnType_JSON,
				Description: "Encryption information for the backup.",
				Transform:   transform.FromField("Backup.EncryptionInfo"),
			},
			{
				Name:        "encryption_information",
				Type:        proto.ColumnType_JSON,
				Description: "All KMS key versions used to encrypt the backup.",
				Transform:   transform.FromField("Backup.EncryptionInformation"),
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Backup.Name").Transform(lastPathElement),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Backup.Name").Transform(spannerSelfLinkToAkas),
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

func listGcpSpannerBackups(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	instance := h.Item.(*instancepb.Instance)
	location := getLastPathElement(instance.Config)

	service, err := SpannerDatabaseAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_backup.listGcpSpannerBackups", "connection_error", err)
		return nil, err
	}

	req := &databasepb.ListBackupsRequest{
		Parent: instance.Name,
	}

	it := service.ListBackups(ctx, req)
	for {
		d.WaitForListRateLimit(ctx)

		resp, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			logger.Error("gcp_spanner_backup.listGcpSpannerBackups", "api_error", err)
			return nil, err
		}

		d.StreamLeafListItem(ctx, &spannerBackupRow{Backup: resp, InstanceLocation: location})

		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}
	return nil, nil
}

//// HYDRATE FUNCTION

func getGcpSpannerBackup(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	name := d.EqualsQualString("name")
	instanceName := d.EqualsQualString("instance_name")

	if name == "" || instanceName == "" {
		return nil, nil
	}

	service, err := SpannerDatabaseAdminService(ctx, d)
	if err != nil {
		logger.Error("gcp_spanner_backup.getGcpSpannerBackup", "connection_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_spanner_backup.getGcpSpannerBackup", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	instanceId := instanceName
	if !strings.HasPrefix(instanceName, "projects/") {
		instanceId = "projects/" + project + "/instances/" + instanceName
	}

	req := &databasepb.GetBackupRequest{
		Name: instanceId + "/backups/" + name,
	}

	resp, err := service.GetBackup(ctx, req)
	if err != nil {
		logger.Error("gcp_spanner_backup.getGcpSpannerBackup", "api_error", err)
		return nil, err
	}

	location, err := fetchSpannerInstanceLocation(ctx, d, instanceId)
	if err != nil {
		logger.Error("gcp_spanner_backup.getGcpSpannerBackup", "instance_location_error", err)
		return nil, err
	}

	return &spannerBackupRow{Backup: resp, InstanceLocation: location}, nil
}

//// TRANSFORM FUNCTIONS

func spannerBackupStateToString(_ context.Context, d *transform.TransformData) (interface{}, error) {
	return databasepb.Backup_State(d.Value.(databasepb.Backup_State)).String(), nil
}

func spannerBackupTimestamps(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerBackupRow)
	param := d.Param.(string)

	var ts interface{}
	switch param {
	case "CreateTime":
		if row.Backup.CreateTime != nil {
			ts = row.Backup.CreateTime.AsTime()
		}
	case "VersionTime":
		if row.Backup.VersionTime != nil {
			ts = row.Backup.VersionTime.AsTime()
		}
	case "ExpireTime":
		if row.Backup.ExpireTime != nil {
			ts = row.Backup.ExpireTime.AsTime()
		}
	case "MaxExpireTime":
		if row.Backup.MaxExpireTime != nil {
			ts = row.Backup.MaxExpireTime.AsTime()
		}
	case "OldestVersionTime":
		if row.Backup.OldestVersionTime != nil {
			ts = row.Backup.OldestVersionTime.AsTime()
		}
	}
	return ts, nil
}

func spannerBackupTurbotData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	row := d.HydrateItem.(*spannerBackupRow)
	param := d.Param.(string)

	// Name format: projects/{project}/instances/{instance}/backups/{backup}
	parts := strings.Split(row.Backup.Name, "/")

	switch param {
	case "InstanceName":
		if len(parts) >= 4 {
			return parts[3], nil
		}
	}
	return nil, nil
}

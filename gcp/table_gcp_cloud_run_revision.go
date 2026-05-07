package gcp

import (
	"context"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/run/v2"
)

//// TABLE DEFINITION

func tableGcpCloudRunRevision(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_run_revision",
		Description: "GCP Cloud Run Revision",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location", "service_name"}),
			Hydrate:    getCloudRunRevision,
			Tags:       map[string]string{"service": "run", "action": "revisions.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listCloudRunRevisions,
			KeyColumns: plugin.KeyColumnSlice{
				{
					Name:    "location",
					Require: plugin.Optional,
				},
				{
					Name:    "service_name",
					Require: plugin.Optional,
				},
			},
			Tags: map[string]string{"service": "run", "action": "revisions.list"},
		},
		GetMatrixItemFunc: BuildCloudRunLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The unique name of this Revision.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "service_name",
				Description: "The short name of the parent service.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunRevisionData, "ServiceName"),
			},
			{
				Name:        "service",
				Description: "The fully qualified name of the parent service.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "uid",
				Description: "Server assigned unique identifier for the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "generation",
				Description: "A number that monotonically increases every time the user modifies the desired state.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "create_time",
				Description: "The creation time.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "update_time",
				Description: "The last-modified time.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "delete_time",
				Description: "The deletion time.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "expire_time",
				Description: "For a deleted resource, the time after which it will be permanently deleted.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "launch_stage",
				Description: "The launch stage as defined by Google Cloud Platform Launch Stages.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "creator",
				Description: "Email address of the authenticated creator.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "etag",
				Description: "A system-generated fingerprint for this version of the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "execution_environment",
				Description: "The sandbox environment to host this Revision.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "encryption_key",
				Description: "A reference to a customer managed encryption key (CMEK) to use to encrypt this container image.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "encryption_key_revocation_action",
				Description: "The action to take if the encryption key is revoked.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "encryption_key_shutdown_duration",
				Description: "If encryption_key_revocation_action is SHUT_DOWN, the duration before shutting down all instances.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "max_instance_request_concurrency",
				Description: "Sets the maximum number of requests that each serving instance can receive.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "timeout",
				Description: "Max allowed time for an instance to respond to a request.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "service_account",
				Description: "Email address of the IAM service account associated with the revision.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "log_uri",
				Description: "The Google Console URI to obtain logs for the Revision.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "observed_generation",
				Description: "The generation of this Revision currently serving traffic.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "reconciling",
				Description: "Indicates whether the resource's reconciliation is still in progress.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "satisfies_pzs",
				Description: "Reserved for future use.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "gpu_zonal_redundancy_disabled",
				Description: "True if GPU zonal redundancy is disabled on this revision.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "session_affinity",
				Description: "Enable session affinity.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "self_link",
				Description: "The server-defined URL for the resource.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     cloudRunRevisionSelfLink,
				Transform:   transform.FromValue(),
			},

			// JSON fields
			{
				Name:        "annotations",
				Description: "Unstructured key value map that may be set by external tools to store arbitrary metadata.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "labels",
				Description: "Unstructured key value map that can be used to organize and categorize objects.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "conditions",
				Description: "The Conditions of all other associated sub-resources.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "containers",
				Description: "Holds the containers that define the unit of execution for this Revision.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "volumes",
				Description: "A list of Volumes to make available to containers.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "vpc_access",
				Description: "VPC Access configuration for this Revision.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "scaling",
				Description: "Scaling settings for this Revision.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "scaling_status",
				Description: "The observed scaling settings for this Revision.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "node_selector",
				Description: "The node selector for the revision.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "service_mesh",
				Description: "Enables Cloud Service Mesh for this revision.",
				Type:        proto.ColumnType_JSON,
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunRevisionData, "Title"),
			},
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Labels"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(cloudRunRevisionData, "Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunRevisionData, "Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunRevisionData, "Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudRunRevisions(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString("location")

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	if matrixLocation != "" {
		location = matrixLocation
	}

	// Minimize API call as per given location
	if region != "" && region != location {
		return nil, nil
	}

	// Create Service Connection
	service, err := CloudRunService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_revision.listCloudRunRevisions", "service_error", err)
		return nil, err
	}

	// Max limit is set as per documentation
	pageSize := types.Int64(500)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	// Use "-" wildcard to list revisions across all services, or filter by service_name if provided
	serviceName := d.EqualsQualString("service_name")
	if serviceName == "" {
		serviceName = "-"
	}

	parent := "projects/" + project + "/locations/" + location + "/services/" + serviceName

	resp := service.Projects.Locations.Services.Revisions.List(parent).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *run.GoogleCloudRunV2ListRevisionsResponse) error {
		// apply rate limiting
		d.WaitForListRateLimit(ctx)

		for _, item := range page.Revisions {
			d.StreamListItem(ctx, item)

			// Check if context has been cancelled or if the limit has been hit (if specified)
			// if there is a limit, it will return the number of rows required to reach this limit
			if d.RowsRemaining(ctx) == 0 {
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 403 {
			plugin.Logger(ctx).Warn("gcp_cloud_run_revision.listCloudRunRevisions", "location_skipped", location, "reason", err)
			return nil, nil
		}
		plugin.Logger(ctx).Error("gcp_cloud_run_revision.listCloudRunRevisions", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getCloudRunRevision(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	// Create Service Connection
	service, err := CloudRunService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_revision.getCloudRunRevision", "service_error", err)
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	revisionName := d.EqualsQuals["name"].GetStringValue()
	location := d.EqualsQuals["location"].GetStringValue()
	serviceName := d.EqualsQuals["service_name"].GetStringValue()

	// Empty Check
	if revisionName == "" || location == "" || serviceName == "" {
		return nil, nil
	}

	input := "projects/" + project + "/locations/" + location + "/services/" + serviceName + "/revisions/" + revisionName

	resp, err := service.Projects.Locations.Services.Revisions.Get(input).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_revision.getCloudRunRevision", "api_error", err)
		return nil, err
	}
	return resp, err
}

//// TRANSFORM FUNCTIONS

func cloudRunRevisionSelfLink(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	data := h.Item.(*run.GoogleCloudRunV2Revision)

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	if matrixLocation != "" {
		location = matrixLocation
	}

	// Name format: projects/{project}/locations/{location}/services/{service}/revisions/{revision}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	serviceName := parts[5]
	name := parts[7]

	selfLink := "https://run.googleapis.com/v2/projects/" + projectID + "/locations/" + location + "/services/" + serviceName + "/revisions/" + name

	return selfLink, nil
}

func cloudRunRevisionData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*run.GoogleCloudRunV2Revision)
	param := h.Param.(string)

	// Name format: projects/{project}/locations/{location}/services/{service}/revisions/{revision}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	location := parts[3]
	serviceName := parts[5]
	name := parts[7]

	turbotData := map[string]interface{}{
		"Project":     projectID,
		"Title":       name,
		"Location":    location,
		"ServiceName": serviceName,
		"Akas":        []string{"gcp://run.googleapis.com/projects/" + projectID + "/locations/" + location + "/services/" + serviceName + "/revisions/" + name},
	}

	return turbotData[param], nil
}

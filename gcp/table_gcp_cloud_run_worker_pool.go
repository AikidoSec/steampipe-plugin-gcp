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

func tableGcpCloudRunWorkerPool(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_run_worker_pool",
		Description: "GCP Cloud Run Worker Pool",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location"}),
			Hydrate:    getCloudRunWorkerPool,
			Tags:       map[string]string{"service": "run", "action": "workerpools.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listCloudRunWorkerPools,
			KeyColumns: plugin.KeyColumnSlice{
				{
					Name:    "location",
					Require: plugin.Optional,
				},
			},
			Tags: map[string]string{"service": "run", "action": "workerpools.list"},
		},
		HydrateConfig: []plugin.HydrateConfig{
			{
				Func: getCloudRunWorkerPoolIamPolicy,
				Tags: map[string]string{"service": "run", "action": "workerpools.getIamPolicy"},
			},
		},
		GetMatrixItemFunc: BuildCloudRunLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The fully qualified name of this Worker Pool.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "client",
				Description: "Arbitrary identifier for the API client.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "client_version",
				Description: "Arbitrary version identifier for the API client.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "creator",
				Description: "Email address of the authenticated creator.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "User-provided description of the Worker Pool.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "etag",
				Description: "A system-generated fingerprint for this version of the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "generation",
				Description: "A number that monotonically increases every time the user modifies the desired state.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "last_modifier",
				Description: "Email address of the last authenticated modifier.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "latest_created_revision",
				Description: "Name of the last created revision.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "latest_ready_revision",
				Description: "Name of the latest revision that is serving traffic.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "launch_stage",
				Description: "The launch stage as defined by Google Cloud Platform Launch Stages.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "observed_generation",
				Description: "The generation of this Worker Pool currently serving traffic.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "reconciling",
				Description: "Returns true if the Worker Pool is currently being acted upon by the system to bring it into the desired state.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "satisfies_pzs",
				Description: "Reserved for future use.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "uid",
				Description: "Server assigned unique identifier for the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "create_time",
				Description: "The creation time.",
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
				Name:        "update_time",
				Description: "The last-modified time.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "self_link",
				Description: "The server-defined URL for the resource.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     cloudRunWorkerPoolSelfLink,
				Transform:   transform.FromValue(),
			},

			// JSON fields
			{
				Name:        "annotations",
				Description: "Unstructured key value map that may be set by external tools to store and arbitrary metadata.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "binary_authorization",
				Description: "Settings for the Binary Authorization feature.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "conditions",
				Description: "The Conditions of all other associated sub-resources.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "custom_audiences",
				Description: "One or more custom audiences that you want this worker pool to support.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "instance_splits",
				Description: "Specifies how to distribute instances over a collection of revisions belonging to the Worker Pool.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "instance_split_statuses",
				Description: "Detailed status information for corresponding instance split targets.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "labels",
				Description: "Unstructured key value map that can be used to organize and categorize objects.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "scaling",
				Description: "Scaling settings for this Worker Pool.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "template",
				Description: "The template used to create revisions for this Worker Pool.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "terminal_condition",
				Description: "The Condition of this Worker Pool, containing its readiness status and detailed error information.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "iam_policy",
				Description: "An Identity and Access Management (IAM) policy, which specifies access controls for Google Cloud resources.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getCloudRunWorkerPoolIamPolicy,
				Transform:   transform.FromValue(),
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunWorkerPoolData, "Title"),
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
				Transform:   transform.FromP(cloudRunWorkerPoolData, "Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunWorkerPoolData, "Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunWorkerPoolData, "Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudRunWorkerPools(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString("region")

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	// Since, when the service API is disabled, matrixLocation value will be nil
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
		plugin.Logger(ctx).Error("gcp_cloud_run_worker_pool.listCloudRunWorkerPools", "service_error", err)
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

	input := "projects/" + project + "/locations/" + location

	resp := service.Projects.Locations.WorkerPools.List(input).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *run.GoogleCloudRunV2ListWorkerPoolsResponse) error {
		// apply rate limiting
		d.WaitForListRateLimit(ctx)

		for _, item := range page.WorkerPools {
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
			plugin.Logger(ctx).Warn("gcp_cloud_run_worker_pool.listCloudRunWorkerPools", "location_skipped", location, "reason", err)
			return nil, nil
		}
		plugin.Logger(ctx).Error("gcp_cloud_run_worker_pool.listCloudRunWorkerPools", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getCloudRunWorkerPool(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	// Create Service Connection
	service, err := CloudRunService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_worker_pool.getCloudRunWorkerPool", "service_error", err)
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	workerPoolName := d.EqualsQuals["name"].GetStringValue()
	location := d.EqualsQuals["location"].GetStringValue()

	// Empty Check
	if workerPoolName == "" || location == "" {
		return nil, nil
	}

	input := "projects/" + project + "/locations/" + location + "/workerPools/" + workerPoolName

	resp, err := service.Projects.Locations.WorkerPools.Get(input).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_worker_pool.getCloudRunWorkerPool", "api_error", err)
		return nil, err
	}
	return resp, err
}

func getCloudRunWorkerPoolIamPolicy(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	data := h.Item.(*run.GoogleCloudRunV2WorkerPool)
	workerPoolName := strings.Split(data.Name, "/")[5]
	location := strings.Split(data.Name, "/")[3]

	// Create Service Connection
	service, err := CloudRunService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_worker_pool.getCloudRunWorkerPoolIamPolicy", "service_error", err)
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	input := "projects/" + project + "/locations/" + location + "/workerPools/" + workerPoolName

	resp, err := service.Projects.Locations.WorkerPools.GetIamPolicy(input).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_worker_pool.getCloudRunWorkerPoolIamPolicy", "api_error", err)
		return nil, err
	}

	return resp, err
}

//// TRANSFORM FUNCTIONS

func cloudRunWorkerPoolSelfLink(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	data := h.Item.(*run.GoogleCloudRunV2WorkerPool)

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	if matrixLocation != "" {
		location = matrixLocation
	}

	// Name format: projects/{project}/locations/{location}/workerPools/{workerPool}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	name := parts[5]

	selfLink := "https://run.googleapis.com/v2/projects/" + projectID + "/locations/" + location + "/workerPools/" + name

	return selfLink, nil
}

func cloudRunWorkerPoolData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*run.GoogleCloudRunV2WorkerPool)
	param := h.Param.(string)

	// Name format: projects/{project}/locations/{location}/workerPools/{workerPool}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	location := parts[3]
	name := parts[5]

	turbotData := map[string]interface{}{
		"Project":  projectID,
		"Title":    name,
		"Location": location,
		"Akas":     []string{"gcp://run.googleapis.com/projects/" + projectID + "/locations/" + location + "/workerPools/" + name},
	}

	return turbotData[param], nil
}

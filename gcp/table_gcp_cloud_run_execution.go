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

func tableGcpCloudRunExecution(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_run_execution",
		Description: "GCP Cloud Run Execution",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location", "job_name"}),
			Hydrate:    getCloudRunExecution,
			Tags:       map[string]string{"service": "run", "action": "executions.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listCloudRunExecutions,
			KeyColumns: plugin.KeyColumnSlice{
				{
					Name:    "location",
					Require: plugin.Optional,
				},
				{
					Name:    "job_name",
					Require: plugin.Optional,
				},
			},
			Tags: map[string]string{"service": "run", "action": "executions.list"},
		},
		GetMatrixItemFunc: BuildCloudRunLocationList,
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The unique name of this Execution.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "job_name",
				Description: "The short name of the parent job.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunExecutionData, "JobName"),
			},
			{
				Name:        "job",
				Description: "The fully qualified name of the parent job.",
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
				Name:        "observed_generation",
				Description: "The generation of this Execution currently serving traffic.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "create_time",
				Description: "The creation time.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "start_time",
				Description: "The time the execution began running.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromGo().NullIfZero(),
			},
			{
				Name:        "completion_time",
				Description: "The time the execution finished running.",
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
				Name:        "launch_stage",
				Description: "The launch stage as defined by Google Cloud Platform Launch Stages.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "log_uri",
				Description: "The Google Console URI to obtain logs for the Execution.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "parallelism",
				Description: "The maximum allowed parallelism for the Execution.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "task_count",
				Description: "The number of tasks the Execution spawns.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "cancelled_count",
				Description: "The number of tasks in the Execution that reached the Cancelled state.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "failed_count",
				Description: "The number of tasks in the Execution that reached the Failed state.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "retried_count",
				Description: "The number of tasks in the Execution that have retried at least once.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "running_count",
				Description: "The number of tasks in the Execution that are currently running.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "succeeded_count",
				Description: "The number of tasks in the Execution that reached the Succeeded state.",
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
				Name:        "self_link",
				Description: "The server-defined URL for the resource.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     cloudRunExecutionSelfLink,
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
				Name:        "template",
				Description: "The template used to create tasks in the Execution.",
				Type:        proto.ColumnType_JSON,
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunExecutionData, "Title"),
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
				Transform:   transform.FromP(cloudRunExecutionData, "Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunExecutionData, "Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(cloudRunExecutionData, "Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudRunExecutions(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString("location")

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
		plugin.Logger(ctx).Error("gcp_cloud_run_execution.listCloudRunExecutions", "service_error", err)
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

	// Use "-" wildcard to list executions across all jobs, or filter by job_name if provided
	jobName := d.EqualsQualString("job_name")
	if jobName == "" {
		jobName = "-"
	}

	parent := "projects/" + project + "/locations/" + location + "/jobs/" + jobName

	resp := service.Projects.Locations.Jobs.Executions.List(parent).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *run.GoogleCloudRunV2ListExecutionsResponse) error {
		// apply rate limiting
		d.WaitForListRateLimit(ctx)

		for _, item := range page.Executions {
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
			plugin.Logger(ctx).Warn("gcp_cloud_run_execution.listCloudRunExecutions", "location_skipped", location, "reason", err)
			return nil, nil
		}
		plugin.Logger(ctx).Error("gcp_cloud_run_execution.listCloudRunExecutions", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getCloudRunExecution(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	// Create Service Connection
	service, err := CloudRunService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_execution.getCloudRunExecution", "service_error", err)
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	executionName := d.EqualsQuals["name"].GetStringValue()
	location := d.EqualsQuals["location"].GetStringValue()
	jobName := d.EqualsQuals["job_name"].GetStringValue()

	// Empty Check
	if executionName == "" || location == "" || jobName == "" {
		return nil, nil
	}

	input := "projects/" + project + "/locations/" + location + "/jobs/" + jobName + "/executions/" + executionName

	resp, err := service.Projects.Locations.Jobs.Executions.Get(input).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_execution.getCloudRunExecution", "api_error", err)
		return nil, err
	}
	return resp, err
}

//// TRANSFORM FUNCTIONS

func cloudRunExecutionSelfLink(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	data := h.Item.(*run.GoogleCloudRunV2Execution)

	var location string
	matrixLocation := d.EqualsQualString(matrixKeyLocation)
	if matrixLocation != "" {
		location = matrixLocation
	}

	// Name format: projects/{project}/locations/{location}/jobs/{job}/executions/{execution}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	jobName := parts[5]
	name := parts[7]

	selfLink := "https://run.googleapis.com/v2/projects/" + projectID + "/locations/" + location + "/jobs/" + jobName + "/executions/" + name

	return selfLink, nil
}

func cloudRunExecutionData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*run.GoogleCloudRunV2Execution)
	param := h.Param.(string)

	// Name format: projects/{project}/locations/{location}/jobs/{job}/executions/{execution}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	location := parts[3]
	jobName := parts[5]
	name := parts[7]

	turbotData := map[string]interface{}{
		"Project":  projectID,
		"Title":    name,
		"Location": location,
		"JobName":  jobName,
		"Akas":     []string{"gcp://run.googleapis.com/projects/" + projectID + "/locations/" + location + "/jobs/" + jobName + "/executions/" + name},
	}

	return turbotData[param], nil
}

package gcp

import (
	"context"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	firebase "google.golang.org/api/firebase/v1beta1"
)

//// TABLE DEFINITION

func tableGcpFirebaseProject(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_firebase_project",
		Description: "GCP Firebase Project",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("project_id"),
			Hydrate:    getFirebaseProject,
			Tags:       map[string]string{"service": "firebase", "action": "projects.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listFirebaseProjects,
			Tags:    map[string]string{"service": "firebase", "action": "projects.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The resource name of the FirebaseProject.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "display_name",
				Description: "The user-assigned display name of the Project.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "project_id",
				Description: "A user-assigned unique identifier for the Project.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "project_number",
				Description: "The globally unique, Google-assigned canonical identifier for the Project.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "etag",
				Description: "A checksum computed by the server based on the value of other fields.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "state",
				Description: "The lifecycle state of the Project.",
				Type:        proto.ColumnType_STRING,
			},

			// JSON fields
			{
				Name:        "annotations",
				Description: "A set of user-defined annotations for the FirebaseProject.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "resources",
				Description: "The default Firebase resources associated with the Project.",
				Type:        proto.ColumnType_JSON,
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
				Transform:   transform.FromP(firebaseProjectData, "Akas"),
			},

			// Standard GCP columns
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ProjectId"),
			},
		},
	}
}

//// LIST FUNCTION

func listFirebaseProjects(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_project.listFirebaseProjects", "service_error", err)
		return nil, err
	}

	pageSize := types.Int64(100)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	resp := service.Projects.List().PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *firebase.ListFirebaseProjectsResponse) error {
		d.WaitForListRateLimit(ctx)

		for _, item := range page.Results {
			d.StreamListItem(ctx, item)

			if d.RowsRemaining(ctx) == 0 {
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_project.listFirebaseProjects", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getFirebaseProject(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_project.getFirebaseProject", "service_error", err)
		return nil, err
	}

	projectId := d.EqualsQuals["project_id"].GetStringValue()
	if projectId == "" {
		return nil, nil
	}

	resp, err := service.Projects.Get("projects/" + projectId).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_project.getFirebaseProject", "api_error", err)
		return nil, err
	}
	return resp, nil
}

//// TRANSFORM FUNCTIONS

func firebaseProjectData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*firebase.FirebaseProject)
	param := h.Param.(string)

	turbotData := map[string]interface{}{
		"Akas": []string{"gcp://firebase.googleapis.com/projects/" + data.ProjectId},
	}

	return turbotData[param], nil
}

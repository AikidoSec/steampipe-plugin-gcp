package gcp

import (
	"context"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	firebase "google.golang.org/api/firebase/v1beta1"
)

//// TABLE DEFINITION

func tableGcpFirebaseWebApp(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_firebase_web_app",
		Description: "GCP Firebase Web App",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("app_id"),
			Hydrate:    getFirebaseWebApp,
			Tags:       map[string]string{"service": "firebase", "action": "projects.webApps.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listFirebaseWebApps,
			Tags:    map[string]string{"service": "firebase", "action": "projects.webApps.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The resource name of the WebApp.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "app_id",
				Description: "The globally unique, Firebase-assigned identifier for the WebApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "display_name",
				Description: "The user-assigned display name for the WebApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "api_key_id",
				Description: "The globally unique, Google-assigned identifier for the Firebase API key associated with the WebApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "web_id",
				Description: "A unique, Firebase-assigned identifier for the WebApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "project_id",
				Description: "A user-assigned unique identifier of the parent FirebaseProject for the WebApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "etag",
				Description: "A checksum computed by the server based on the value of other fields.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "expire_time",
				Description: "The time the App is considered expired and will be permanently deleted.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "state",
				Description: "The lifecycle state of the App.",
				Type:        proto.ColumnType_STRING,
			},

			// JSON fields
			{
				Name:        "app_urls",
				Description: "The URLs where the WebApp is hosted.",
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
				Transform:   transform.FromP(firebaseWebAppData, "Akas"),
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

func listFirebaseWebApps(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_web_app.listFirebaseWebApps", "service_error", err)
		return nil, err
	}

	pageSize := types.Int64(100)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	resp := service.Projects.WebApps.List("projects/" + project).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *firebase.ListWebAppsResponse) error {
		d.WaitForListRateLimit(ctx)

		for _, item := range page.Apps {
			d.StreamListItem(ctx, item)

			if d.RowsRemaining(ctx) == 0 {
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_web_app.listFirebaseWebApps", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getFirebaseWebApp(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_web_app.getFirebaseWebApp", "service_error", err)
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	appId := d.EqualsQuals["app_id"].GetStringValue()
	if appId == "" {
		return nil, nil
	}

	input := "projects/" + project + "/webApps/" + appId

	resp, err := service.Projects.WebApps.Get(input).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_web_app.getFirebaseWebApp", "api_error", err)
		return nil, err
	}
	return resp, nil
}

//// TRANSFORM FUNCTIONS

func firebaseWebAppData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*firebase.WebApp)
	param := h.Param.(string)

	// Name format: projects/{projectId}/webApps/{appId}
	parts := strings.Split(data.Name, "/")
	projectID := parts[1]
	appType := parts[2]
	appId := parts[3]

	turbotData := map[string]interface{}{
		"Akas": []string{"gcp://firebase.googleapis.com/projects/" + projectID + "/" + appType + "/" + appId},
	}

	return turbotData[param], nil
}

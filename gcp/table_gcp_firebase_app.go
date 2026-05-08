package gcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	firebase "google.golang.org/api/firebase/v1beta1"
)

//// TABLE DEFINITION

func tableGcpFirebaseApp(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_firebase_app",
		Description: "GCP Firebase App — a unified view of all Firebase apps (Android, iOS, and Web) within a project.",
		List: &plugin.ListConfig{
			Hydrate: listFirebaseApps,
			Tags:    map[string]string{"service": "firebase", "action": "projects.searchApps"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The resource name of the Firebase App.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "app_id",
				Description: "The globally unique, Firebase-assigned identifier for the App.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "display_name",
				Description: "The user-assigned display name of the Firebase App.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "platform",
				Description: "The platform of the Firebase App (IOS, ANDROID, or WEB).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "namespace",
				Description: "The platform-specific identifier of the App: package name for Android, bundle ID for iOS, webId for Web.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "api_key_id",
				Description: "The globally unique, Google-assigned identifier for the Firebase API key associated with the App.",
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
				Transform:   transform.FromP(firebaseAppInfoData, "Akas"),
			},

			// Standard GCP columns
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(firebaseAppInfoData, "Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listFirebaseApps(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_app.listFirebaseApps", "service_error", err)
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

	resp := service.Projects.SearchApps("projects/" + project).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *firebase.SearchFirebaseAppsResponse) error {
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
		plugin.Logger(ctx).Error("gcp_firebase_app.listFirebaseApps", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// TRANSFORM FUNCTIONS

func firebaseAppInfoData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*firebase.FirebaseAppInfo)
	param := h.Param.(string)

	// Name format: projects/{projectId}/{platform}Apps/{appId}
	parts := strings.Split(data.Name, "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("unexpected FirebaseAppInfo name format: %q", data.Name)
	}
	projectID := parts[1]
	appType := parts[2]
	appId := parts[3]

	turbotData := map[string]interface{}{
		"Project": projectID,
		"Akas":    []string{"gcp://firebase.googleapis.com/projects/" + projectID + "/" + appType + "/" + appId},
	}

	return turbotData[param], nil
}

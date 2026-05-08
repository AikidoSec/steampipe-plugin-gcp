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

func tableGcpFirebaseAndroidApp(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_firebase_android_app",
		Description: "GCP Firebase Android App",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("app_id"),
			Hydrate:    getFirebaseAndroidApp,
			Tags:       map[string]string{"service": "firebase", "action": "projects.androidApps.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listFirebaseAndroidApps,
			Tags:    map[string]string{"service": "firebase", "action": "projects.androidApps.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Description: "The resource name of the AndroidApp.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
			},
			{
				Name:        "app_id",
				Description: "The globally unique, Firebase-assigned identifier for the AndroidApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "display_name",
				Description: "The user-assigned display name for the AndroidApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "api_key_id",
				Description: "The globally unique, Google-assigned identifier for the Firebase API key associated with the AndroidApp.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "package_name",
				Description: "The canonical package name of the Android app as it would appear in the Google Play Developer Console.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "project_id",
				Description: "A user-assigned unique identifier of the parent FirebaseProject for the AndroidApp.",
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
				Name:        "sha1_hashes",
				Description: "The SHA1 certificate hashes for the AndroidApp.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "sha256_hashes",
				Description: "The SHA256 certificate hashes for the AndroidApp.",
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
				Transform:   transform.FromP(firebaseAndroidAppData, "Akas"),
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

func listFirebaseAndroidApps(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_android_app.listFirebaseAndroidApps", "service_error", err)
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

	resp := service.Projects.AndroidApps.List("projects/" + project).PageSize(*pageSize)
	if err := resp.Pages(ctx, func(page *firebase.ListAndroidAppsResponse) error {
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
		plugin.Logger(ctx).Error("gcp_firebase_android_app.listFirebaseAndroidApps", "api_error", err)
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getFirebaseAndroidApp(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := FirebaseService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_android_app.getFirebaseAndroidApp", "service_error", err)
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

	input := "projects/" + project + "/androidApps/" + appId

	resp, err := service.Projects.AndroidApps.Get(input).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_firebase_android_app.getFirebaseAndroidApp", "api_error", err)
		return nil, err
	}
	return resp, nil
}

//// TRANSFORM FUNCTIONS

func firebaseAndroidAppData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*firebase.AndroidApp)
	param := h.Param.(string)

	// Name format: projects/{projectId}/androidApps/{appId}
	parts := strings.Split(data.Name, "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("unexpected AndroidApp name format: %q", data.Name)
	}
	projectID := parts[1]
	appType := parts[2]
	appId := parts[3]

	turbotData := map[string]interface{}{
		"Akas": []string{"gcp://firebase.googleapis.com/projects/" + projectID + "/" + appType + "/" + appId},
	}

	return turbotData[param], nil
}

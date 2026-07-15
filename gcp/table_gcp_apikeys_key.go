package gcp

import (
	"context"
	"encoding/json"

	"github.com/hashicorp/go-multierror"
	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/apikeys/v2"
	"google.golang.org/api/cloudasset/v1"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

//// TABLE DEFINITION

func tableGcpApiKeysKey(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_apikeys_key",
		Description: "GCP API Keys Key",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getApiKeysKey,
			Tags:       map[string]string{"service": "apikeys", "action": "keys.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listApiKeysKeys,
			Tags:    map[string]string{"service": "apikeys", "action": "keys.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name").Transform(lastPathElement),
				Description: "The resource name of the key.",
			},
			{
				Name:        "uid",
				Type:        proto.ColumnType_STRING,
				Description: "Unique id in UUID4 format.",
			},
			{
				Name:        "create_time",
				Description: "A timestamp identifying the time this key was originally created.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "delete_time",
				Description: "A timestamp when this key was deleted.",
				Transform:   transform.FromField("DeleteTime").Transform(transform.NullIfZeroValue),
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "display_name",
				Type:        proto.ColumnType_STRING,
				Description: "Human-readable display name of this key that you can modify.",
			},
			{
				Name:        "etag",
				Description: "A checksum computed by the server based on the current value of the Key resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "update_time",
				Description: "A timestamp identifying the time this key was last updated.",
				Type:        proto.ColumnType_TIMESTAMP,
			},

			// JSON columns
			{
				Name:        "annotations",
				Description: "Annotations is an unstructured key-value map stored with a policy that may be set by external tools to store and retrieve arbitrary metadata.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "restrictions",
				Description: "The restrictions on the key.",
				Type:        proto.ColumnType_JSON,
			},

			// standard steampipe columns
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
				Transform:   transform.FromP(gcpApiKeyTurbotData, "Akas"),
			},

			// standard gcp columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromConstant("global"),
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

//// FETCH FUNCTIONS

func listApiKeysKeys(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)
	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_api_key.listApiKeysKeys", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	// Page size should be in range of [0, 300].
	pageSize := types.Int64(300)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil {
		if *limit < *pageSize {
			pageSize = limit
		}
	}

	// Create Service Connection
	service, err := APIKeysService(ctx, d)
	if err != nil {
		logger.Error("gcp_api_key.listApiKeysKeys", "service_error", err)
		return nil, err
	}

	// NOTE: Key is a global resource; hence the only supported value for location is `global`.
	resp := service.Projects.Locations.Keys.List("projects/" + project + "/locations/global").PageSize(*pageSize)

	var keys []*apikeys.V2Key
	if err := resp.Pages(
		ctx,
		func(page *apikeys.V2ListKeysResponse) error {
			// apply rate limiting
			d.WaitForListRateLimit(ctx)

			for _, item := range page.Keys {
				keys = append(keys, item)

				// Check if context has been cancelled or if the limit has been hit (if specified)
				// if there is a limit, it will return the number of rows required to reach this limit
				if d.RowsRemaining(ctx) == 0 {
					page.NextPageToken = ""
					return nil
				}
			}
			return nil
		},
	); err != nil {
		var cErr error
		keys, cErr = listWithCloudAssetSearchAllResources(ctx, d, project)
		if cErr != nil {
			err = multierror.Append(err, cErr)
			logger.Error("gcp_api_key.listApiKeysKeys", "api_errors", err)
			return nil, err
		}
	}

	for _, item := range keys {
		d.StreamListItem(ctx, item)

		if d.RowsRemaining(ctx) == 0 {
			return nil, nil
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getApiKeysKey(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	logger := plugin.Logger(ctx)

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		logger.Error("gcp_api_key.getApiKeysKey", "cache_error", err)
		return nil, err
	}
	project := projectId.(string)

	name := d.EqualsQuals["name"].GetStringValue()

	// Validate - name should not be blank
	if name == "" {
		return nil, nil
	}

	// Create Service Connection
	service, err := APIKeysService(ctx, d)
	if err != nil {
		logger.Error("gcp_api_key.getApiKeysKey", "service_error", err)
		return nil, err
	}

	// NOTE: Key is a global resource; hence the only supported value for location is `global`.
	op, err := service.Projects.Locations.Keys.Get("projects/" + project + "/locations/global/keys/" + name).Do()
	if err != nil {
		logger.Error("gcp_api_key.getApiKeysKey", "api_error", err)
		return nil, err
	}
	return op, nil
}

// /// TRANSFORM FUNCTIONS

func gcpApiKeyTurbotData(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	keyData := d.HydrateItem.(*apikeys.V2Key)
	akas := []string{"gcp://iam.googleapis.com/" + keyData.Name}
	return akas, nil
}

func listWithCloudAssetSearchAllResources(ctx context.Context, d *plugin.QueryData, project string) ([]*apikeys.V2Key, error) {
	logger := plugin.Logger(ctx)

	// Create Service Connection
	service, err := CloudAssetService(ctx, d)
	if err != nil {
		logger.Error("gcp_api_key.listApiKeysKeys", "service_error", err)
		return nil, err
	}

	var keys []*apikeys.V2Key
	pageSize := new(int64(500))
	resp := service.V1.SearchAllResources("projects/" + project).
		AssetTypes("apikeys.googleapis.com/Key").
		ReadMask("name,assetType,displayName,location,createTime,updateTime,versionedResources").
		PageSize(*pageSize)

	if err := resp.Pages(ctx, func(page *cloudasset.SearchAllResourcesResponse) error {
		d.WaitForListRateLimit(ctx)

		for _, item := range page.Results {
			key, err := apiKeyFromSearchResult(item)
			if err != nil {
				return err
			}

			if key == nil {
				continue
			}

			keys = append(keys, key)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return keys, nil
}

func apiKeyFromSearchResult(item *cloudasset.ResourceSearchResult) (*apikeys.V2Key, error) {
	if item == nil {
		return nil, nil
	}

	for _, vr := range item.VersionedResources {
		if vr == nil || len(vr.Resource) == 0 {
			continue
		}

		if vr.Version != "" && vr.Version != "v2" {
			continue
		}

		key := &apikeys.V2Key{}
		if err := json.Unmarshal(vr.Resource, key); err != nil {
			return nil, err
		}
		return key, nil
	}

	return nil, nil
}

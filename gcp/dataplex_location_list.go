package gcp

import (
	"context"
	"slices"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// func init() {
// 	pluginQueryData = &plugin.QueryData{
// 		ConnectionManager: connection.NewManager(),
// 	}
// }

// BuildDataplexLocationList :: return a list of matrix items, one per region specified
func BuildDataplexLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {

	// have we already created and cached the locations?
	locationCacheKey := "Dataplex"
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("listlocationDetails:", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	var ignoredLocations []string
	val := ctx.Value("ignoredLocations")
	if val != nil {
		var ok bool
		ignoredLocations, ok = val.([]string)
		if !ok {
			plugin.Logger(ctx).Error("BuildDataplexLocationList", "type_assertion_error", val)
		}
	}

	// Create Service Connection
	service, err := DataplexService(ctx, d)
	if err != nil {
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		return nil
	}
	project := projectData.Project

	resp, err := service.Projects.Locations.List("projects/" + project).Do()
	if err != nil {
		return nil
	}

	// validate location list
	matrix := make([]map[string]interface{}, 0, len(resp.Locations))
	for _, location := range resp.Locations {
		if slices.Contains(ignoredLocations, location.LocationId) {
			continue
		}

		matrix = append(matrix, map[string]interface{}{matrixKeyLocation: location.LocationId})
	}
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)
	return matrix
}

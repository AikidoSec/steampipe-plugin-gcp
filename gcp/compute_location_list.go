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

// BuildregionList :: return a list of matrix items, one per region specified
// https://cloud.google.com/dataproc/docs/concepts/regional-endpoints
func BuildComputeLocationList(ctx context.Context, d *plugin.QueryData) []map[string]any {

	// have we already created and cached the locations?
	locationCacheKey := "Compute"
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("listlocationDetails:", cachedData.([]map[string]any))
		return cachedData.([]map[string]any)
	}

	var ignoredLocations []string
	val := ctx.Value("ignoredLocations")
	if val != nil {
		var ok bool
		ignoredLocations, ok = val.([]string)
		if !ok {
			plugin.Logger(ctx).Error("BuildComputeLocationList", "type_assertion_error", val)
		}
	}

	// Create Service Connection
	service, err := ComputeService(ctx, d)
	if err != nil {
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		return nil
	}
	project := projectData.Project

	resp, err := service.Regions.List(project).Do()
	if err != nil {
		return nil
	}

	// validate location list
	matrix := make([]map[string]any, 0, len(resp.Items))
	for _, location := range resp.Items {
		if slices.Contains(ignoredLocations, location.Name) {
			continue
		}

		matrix = append(matrix, map[string]any{matrixKeyLocation: location.Name})
	}
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)
	return matrix
}

// BuildComputeLocationListWithGlobal :: return a list of matrix items including global and all regions
func BuildComputeLocationListWithGlobal(ctx context.Context, d *plugin.QueryData) []map[string]any {
	// have we already created and cached the locations?
	locationCacheKey := "ComputeWithGlobal"
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Trace("listlocationDetails:", cachedData.([]map[string]any))
		return cachedData.([]map[string]any)
	}

	var ignoredLocations []string
	val := ctx.Value("ignoredLocations")
	if val != nil {
		var ok bool
		ignoredLocations, ok = val.([]string)
		if !ok {
			plugin.Logger(ctx).Error("BuildComputeLocationListWithGlobal", "type_assertion_error", val)
		}
	}

	// Create Service Connection
	service, err := ComputeService(ctx, d)
	if err != nil {
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		return nil
	}
	project := projectData.Project

	resp, err := service.Regions.List(project).Do()
	if err != nil {
		return nil
	}

	// Add global and all regions to the matrix
	matrix := make([]map[string]any, 0, len(resp.Items)+1)
	// Add global first
	matrix = append(matrix, map[string]any{matrixKeyLocation: "global"})
	// Then add all regions
	for _, location := range resp.Items {
		if slices.Contains(ignoredLocations, location.Name) {
			continue
		}

		matrix = append(matrix, map[string]any{matrixKeyLocation: location.Name})
	}
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)
	return matrix
}

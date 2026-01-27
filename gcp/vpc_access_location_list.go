package gcp

import (
	"context"
	"slices"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"google.golang.org/api/vpcaccess/v1"
)

// BuildregionList :: return a list of matrix items, one per region specified
func BuildVPCAccessLocationList(ctx context.Context, d *plugin.QueryData) []map[string]interface{} {

	// have we already created and cached the locations?
	locationCacheKey := "BuildVPCAccessLocationList"
	if cachedData, ok := d.ConnectionManager.Cache.Get(locationCacheKey); ok {
		plugin.Logger(ctx).Debug("BuildVPCAccessLocationList:", cachedData.([]map[string]interface{}))
		return cachedData.([]map[string]interface{})
	}

	var ignoredLocations []string
	val := ctx.Value("ignoredLocations")
	if val != nil {
		var ok bool
		ignoredLocations, ok = val.([]string)
		if !ok {
			plugin.Logger(ctx).Error("BuildCloudRunLocationList", "type_assertion_error", val)
		}
	}

	// Create Service Connection
	service, err := VPCAccessService(ctx, d)
	if err != nil {
		return nil
	}

	// Get project details
	projectData, err := activeProject(ctx, d)
	if err != nil {
		return nil
	}
	project := projectData.Project

	resp := service.Projects.Locations.List("projects/" + project)
	var locations []*vpcaccess.Location

	if err := resp.Pages(ctx, func(page *vpcaccess.ListLocationsResponse) error {
		locations = append(locations, page.Locations...)
		return nil
	}); err != nil {
		return nil
	}

	// validate location list
	matrix := make([]map[string]interface{}, 0, len(locations))
	for _, location := range locations {
		if slices.Contains(ignoredLocations, location.LocationId) {
			continue
		}

		matrix = append(matrix, map[string]interface{}{matrixKeyLocation: location.LocationId})
	}
	d.ConnectionManager.Cache.Set(locationCacheKey, matrix)
	return matrix
}

package gcp

import (
	"context"
	"strings"
	"sync"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/compute/v1"
)

func tableGcpComputeNetworkEndpointGroup(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_compute_network_endpoint_group",
		Description: "GCP Compute Network Endpoint Group",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getComputeNetworkEndpointGroup,
			Tags:       map[string]string{"service": "compute", "action": "networkEndpointGroups.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listComputeNetworkEndpointGroups,
			Tags:    map[string]string{"service": "compute", "action": "networkEndpointGroups.list"},
		},
		Columns: []*plugin.Column{
			{
				Name:        "name",
				Type:        proto.ColumnType_STRING,
				Description: "Name of the network endpoint group.",
			},
			{
				Name:        "id",
				Type:        proto.ColumnType_INT,
				Description: "The unique identifier for the resource.",
			},
			{
				Name:        "creation_timestamp",
				Type:        proto.ColumnType_TIMESTAMP,
				Description: "Creation timestamp in RFC3339 text format.",
			},
			{
				Name:        "default_port",
				Type:        proto.ColumnType_INT,
				Description: "The default port used if the port number is not specified in the network endpoint.",
			},
			{
				Name:        "description",
				Type:        proto.ColumnType_STRING,
				Description: "An optional description of this resource.",
			},
			{
				Name:        "kind",
				Type:        proto.ColumnType_STRING,
				Description: "Type of the resource. Always compute#networkEndpointGroup for network endpoint groups.",
			},
			{
				Name:        "network",
				Type:        proto.ColumnType_STRING,
				Description: "The URL of the network to which all network endpoints in the NEG belong.",
			},
			{
				Name:        "network_endpoint_type",
				Type:        proto.ColumnType_STRING,
				Description: "Type of network endpoints in this network endpoint group.",
			},
			{
				Name:        "psc_target_service",
				Type:        proto.ColumnType_STRING,
				Description: "The target service URL used to set up private service connection.",
			},
			{
				Name:        "region",
				Type:        proto.ColumnType_STRING,
				Description: "The URL of the region where the network endpoint group is located.",
			},
			{
				Name:        "region_name",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Region").Transform(lastPathElement),
				Description: "The region name where the network endpoint group is located.",
			},
			{
				Name:        "self_link",
				Type:        proto.ColumnType_STRING,
				Description: "Server-defined URL for the resource.",
			},
			{
				Name:        "size",
				Type:        proto.ColumnType_INT,
				Description: "Number of network endpoints in the network endpoint group.",
			},
			{
				Name:        "subnetwork",
				Type:        proto.ColumnType_STRING,
				Description: "The URL of the subnetwork to which all network endpoints in the NEG belong.",
			},
			{
				Name:        "zone",
				Type:        proto.ColumnType_STRING,
				Description: "The URL of the zone where the network endpoint group is located.",
			},
			{
				Name:        "zone_name",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Zone").Transform(lastPathElement),
				Description: "The zone name where the network endpoint group is located.",
			},
			{
				Name:        "annotations",
				Type:        proto.ColumnType_JSON,
				Description: "Metadata defined as annotations on the network endpoint group.",
			},
			{
				Name:        "app_engine",
				Type:        proto.ColumnType_JSON,
				Description: "App Engine configuration when the NEG targets serverless endpoints.",
			},
			{
				Name:        "cloud_function",
				Type:        proto.ColumnType_JSON,
				Description: "Cloud Function configuration when the NEG targets serverless endpoints.",
			},
			{
				Name:        "cloud_run",
				Type:        proto.ColumnType_JSON,
				Description: "Cloud Run configuration when the NEG targets serverless endpoints.",
			},
			{
				Name:        "psc_data",
				Type:        proto.ColumnType_JSON,
				Description: "Private Service Connect data for the network endpoint group.",
			},
			{
				Name:        "location_type",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpComputeNetworkEndpointGroupData, "LocationType"),
				Description: "Scope of the resource: global, region, or zone.",
			},

			// standard steampipe columns
			{
				Name:        "title",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
				Description: ColumnDescriptionTitle,
			},
			{
				Name:        "akas",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(gcpComputeNetworkEndpointGroupData, "Akas"),
				Description: ColumnDescriptionAkas,
			},

			// standard gcp columns
			{
				Name:        "location",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpComputeNetworkEndpointGroupData, "Location"),
				Description: ColumnDescriptionLocation,
			},
			{
				Name:        "project",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromP(gcpComputeNetworkEndpointGroupData, "Project"),
				Description: ColumnDescriptionProject,
			},
		},
	}
}

func listComputeNetworkEndpointGroups(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := ComputeService(ctx, d)
	if err != nil {
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	pageSize := types.Int64(500)
	limit := d.QueryContext.Limit
	if d.QueryContext.Limit != nil && *limit < *pageSize {
		pageSize = limit
	}

	globalCall := service.GlobalNetworkEndpointGroups.List(project).MaxResults(*pageSize)
	if err := globalCall.Pages(ctx, func(page *compute.NetworkEndpointGroupList) error {
		d.WaitForListRateLimit(ctx)
		for _, item := range page.Items {
			d.StreamListItem(ctx, item)
			if d.RowsRemaining(ctx) == 0 {
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		plugin.Logger(ctx).Error("gcp_compute_network_endpoint_group.listComputeNetworkEndpointGroups", "global_api_error", err)
		return nil, err
	}

	wg := sync.WaitGroup{}
	for _, matrixItem := range BuildComputeLocationList(ctx, d) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			region := matrixItem[matrixKeyLocation].(string)
			regionalCall := service.RegionNetworkEndpointGroups.List(project, region).MaxResults(*pageSize)

			if err := regionalCall.Pages(ctx, func(page *compute.NetworkEndpointGroupList) error {
				d.WaitForListRateLimit(ctx)
				for _, item := range page.Items {
					d.StreamListItem(ctx, item)
					if d.RowsRemaining(ctx) == 0 {
						page.NextPageToken = ""
						return nil
					}
				}
				return nil
			}); err != nil {
				plugin.Logger(ctx).Error("gcp_compute_network_endpoint_group.listComputeNetworkEndpointGroups", "regional_api_error", err, "region", region)
				return
			}
			if d.RowsRemaining(ctx) == 0 {
				return
			}
		}()
	}
	wg.Wait()

	zonalCall := service.NetworkEndpointGroups.AggregatedList(project).MaxResults(*pageSize)
	if err := zonalCall.Pages(ctx, func(page *compute.NetworkEndpointGroupAggregatedList) error {
		d.WaitForListRateLimit(ctx)
		for _, scopedList := range page.Items {
			for _, item := range scopedList.NetworkEndpointGroups {
				d.StreamListItem(ctx, item)
				if d.RowsRemaining(ctx) == 0 {
					page.NextPageToken = ""
					return nil
				}
			}
		}
		return nil
	}); err != nil {
		plugin.Logger(ctx).Error("gcp_compute_network_endpoint_group.listComputeNetworkEndpointGroups", "zonal_api_error", err)
		return nil, err
	}

	return nil, nil
}

func getComputeNetworkEndpointGroup(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	service, err := ComputeService(ctx, d)
	if err != nil {
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	name := d.EqualsQualString("name")
	if name == "" {
		return nil, nil
	}

	globalCall := service.GlobalNetworkEndpointGroups.List(project).Filter("name = \"" + name + "\"")
	var neg *compute.NetworkEndpointGroup
	if err := globalCall.Pages(ctx, func(page *compute.NetworkEndpointGroupList) error {
		for _, item := range page.Items {
			if item.Name == name {
				neg = item
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if neg != nil {
		return neg, nil
	}

	for _, matrixItem := range BuildComputeLocationList(ctx, d) {
		region := matrixItem[matrixKeyLocation].(string)
		regionalCall := service.RegionNetworkEndpointGroups.List(project, region).Filter("name = \"" + name + "\"")
		if err := regionalCall.Pages(ctx, func(page *compute.NetworkEndpointGroupList) error {
			for _, item := range page.Items {
				if item.Name == name {
					neg = item
					page.NextPageToken = ""
					return nil
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		if neg != nil {
			return neg, nil
		}
	}

	zonalCall := service.NetworkEndpointGroups.AggregatedList(project).Filter("name = \"" + name + "\"")
	if err := zonalCall.Pages(ctx, func(page *compute.NetworkEndpointGroupAggregatedList) error {
		for _, scopedList := range page.Items {
			for _, item := range scopedList.NetworkEndpointGroups {
				if item.Name == name {
					neg = item
					page.NextPageToken = ""
					return nil
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return neg, nil
}

func gcpComputeNetworkEndpointGroupData(_ context.Context, d *transform.TransformData) (interface{}, error) {
	data := d.HydrateItem.(*compute.NetworkEndpointGroup)
	project := ""
	location := "global"
	locationType := "global"

	if data.SelfLink != "" {
		parts := strings.Split(data.SelfLink, "/")
		if len(parts) > 6 {
			project = parts[6]
		}
		for i, part := range parts {
			if part == "zones" && i < len(parts)-1 {
				location = parts[i+1]
				locationType = "zone"
				break
			}
			if part == "regions" && i < len(parts)-1 {
				location = parts[i+1]
				locationType = "region"
				break
			}
		}
	}

	turbotData := map[string]interface{}{
		"Project":      project,
		"Location":     location,
		"LocationType": locationType,
		"Akas":         []string{strings.ReplaceAll(data.SelfLink, "https://www.googleapis.com/compute/v1/", "gcp://compute.googleapis.com/")},
	}

	return turbotData[d.Param.(string)], nil
}

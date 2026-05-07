package gcp

import (
	"context"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"

	"google.golang.org/api/googleapi"
	run1 "google.golang.org/api/run/v1"
)

// cloudRunDomainMappingInfo wraps DomainMapping with location and project,
// which are not embedded in the v1 ObjectMeta response.
type cloudRunDomainMappingInfo struct {
	*run1.DomainMapping
	Location string
	Project  string
}

//// TABLE DEFINITION

func tableGcpCloudRunDomainMapping(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_cloud_run_domain_mapping",
		Description: "GCP Cloud Run Domain Mapping",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "location"}),
			Hydrate:    getCloudRunDomainMapping,
			Tags:       map[string]string{"service": "run", "action": "domainmappings.get"},
		},
		List: &plugin.ListConfig{
			Hydrate: listCloudRunDomainMappings,
			KeyColumns: plugin.KeyColumnSlice{
				{
					Name:    "location",
					Require: plugin.Optional,
				},
			},
			Tags: map[string]string{"service": "run", "action": "domainmappings.list"},
		},
		GetMatrixItemFunc: BuildCloudRunLocationList,
		Columns: []*plugin.Column{
			// ObjectMeta fields
			{
				Name:        "name",
				Description: "The domain name of the mapping (e.g. example.com).",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Metadata.Name"),
			},
			{
				Name:        "namespace",
				Description: "The namespace (project number) in which the domain mapping lives.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Metadata.Namespace"),
			},
			{
				Name:        "uid",
				Description: "Server assigned unique identifier for the resource.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Metadata.Uid"),
			},
			{
				Name:        "generation",
				Description: "A sequence number representing a specific generation of the desired state.",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromField("Metadata.Generation"),
			},
			{
				Name:        "resource_version",
				Description: "An opaque value that represents the internal version of this object.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Metadata.ResourceVersion"),
			},
			{
				Name:        "self_link",
				Description: "The server-defined URL for the resource.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Metadata.SelfLink"),
			},
			{
				Name:        "create_time",
				Description: "The creation time.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("Metadata.CreationTimestamp").NullIfZero(),
			},
			{
				Name:        "delete_time",
				Description: "The deletion time. Only populated during a deletion.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("Metadata.DeletionTimestamp").NullIfZero(),
			},
			{
				Name:        "deletion_grace_period_seconds",
				Description: "The number of seconds the object should wait before being deleted.",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromField("Metadata.DeletionGracePeriodSeconds"),
			},

			// DomainMappingSpec fields
			{
				Name:        "certificate_mode",
				Description: "The mode of the certificate (e.g. AUTOMATIC, NONE).",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Spec.CertificateMode"),
			},
			{
				Name:        "force_override",
				Description: "If true, the mapping will override any existing mapping for this domain.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("Spec.ForceOverride"),
			},
			{
				Name:        "route_name",
				Description: "The name of the Cloud Run service the domain is mapped to.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Spec.RouteName"),
			},

			// DomainMappingStatus fields
			{
				Name:        "mapped_route_name",
				Description: "The name of the route the domain mapping is currently mapped to.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Status.MappedRouteName"),
			},
			{
				Name:        "observed_generation",
				Description: "The generation of this mapping currently serving traffic.",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromField("Status.ObservedGeneration"),
			},
			{
				Name:        "url",
				Description: "The URL of the domain mapping.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Status.Url"),
			},

			// JSON fields
			{
				Name:        "annotations",
				Description: "Unstructured key value map that may be set by external tools to store arbitrary metadata.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Metadata.Annotations"),
			},
			{
				Name:        "labels",
				Description: "Unstructured key value map that can be used to organize and categorize objects.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Metadata.Labels"),
			},
			{
				Name:        "owner_references",
				Description: "List of objects that own this object.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Metadata.OwnerReferences"),
			},
			{
				Name:        "finalizers",
				Description: "List of finalizers attached to the object.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Metadata.Finalizers"),
			},
			{
				Name:        "conditions",
				Description: "Array of observed conditions for the domain mapping.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Status.Conditions"),
			},
			{
				Name:        "resource_records",
				Description: "DNS records to configure when the domain mapping is active.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Status.ResourceRecords"),
			},

			// Standard steampipe columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Metadata.Name"),
			},
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Metadata.Labels"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromP(cloudRunDomainMappingData, "Akas"),
			},

			// Standard GCP columns
			{
				Name:        "location",
				Description: ColumnDescriptionLocation,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Location"),
			},
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Project"),
			},
		},
	}
}

//// LIST FUNCTION

func listCloudRunDomainMappings(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
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
	service, err := CloudRunServiceV1(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_domain_mapping.listCloudRunDomainMappings", "service_error", err)
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

	parent := "projects/" + project + "/locations/" + location

	// The v1 API uses Kubernetes-style Continue token pagination rather than PageToken
	var continueToken string
	for {
		req := service.Projects.Locations.Domainmappings.List(parent).Limit(*pageSize)
		if continueToken != "" {
			req = req.Continue(continueToken)
		}

		resp, err := req.Do()
		if err != nil {
			if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 403 {
				plugin.Logger(ctx).Warn("gcp_cloud_run_domain_mapping.listCloudRunDomainMappings", "location_skipped", location, "reason", err)
				return nil, nil
			}
			plugin.Logger(ctx).Error("gcp_cloud_run_domain_mapping.listCloudRunDomainMappings", "api_error", err)
			return nil, err
		}

		// apply rate limiting
		d.WaitForListRateLimit(ctx)

		for _, item := range resp.Items {
			d.StreamListItem(ctx, &cloudRunDomainMappingInfo{
				DomainMapping: item,
				Location:      location,
				Project:       project,
			})

			// Check if context has been cancelled or if the limit has been hit (if specified)
			// if there is a limit, it will return the number of rows required to reach this limit
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}

		if resp.Metadata == nil || resp.Metadata.Continue == "" {
			break
		}
		continueToken = resp.Metadata.Continue
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getCloudRunDomainMapping(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	// Create Service Connection
	service, err := CloudRunServiceV1(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_domain_mapping.getCloudRunDomainMapping", "service_error", err)
		return nil, err
	}

	// Get project details
	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	domainName := d.EqualsQuals["name"].GetStringValue()
	location := d.EqualsQuals["location"].GetStringValue()

	// Empty Check
	if domainName == "" || location == "" {
		return nil, nil
	}

	name := "projects/" + project + "/locations/" + location + "/domainmappings/" + domainName

	resp, err := service.Projects.Locations.Domainmappings.Get(name).Do()
	if err != nil {
		plugin.Logger(ctx).Error("gcp_cloud_run_domain_mapping.getCloudRunDomainMapping", "api_error", err)
		return nil, err
	}

	return &cloudRunDomainMappingInfo{
		DomainMapping: resp,
		Location:      location,
		Project:       project,
	}, nil
}

//// TRANSFORM FUNCTIONS

func cloudRunDomainMappingData(ctx context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*cloudRunDomainMappingInfo)
	param := h.Param.(string)

	turbotData := map[string]interface{}{
		"Akas": []string{"gcp://run.googleapis.com/projects/" + data.Project + "/locations/" + data.Location + "/domainmappings/" + data.Metadata.Name},
	}

	return turbotData[param], nil
}

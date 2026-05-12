package gcp

import (
	"context"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/bigquery/v2"
)

//// TABLE DEFINITION

func tableGcpBigQueryRoutine(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "gcp_bigquery_routine",
		Description: "GCP BigQuery Routine",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"dataset_id", "routine_id"}),
			Hydrate:    getBigQueryRoutine,
			Tags:       map[string]string{"service": "bigquery", "action": "routines.get"},
		},
		List: &plugin.ListConfig{
			ParentHydrate: listBigQueryDatasets,
			Hydrate:       listBigQueryRoutines,
			Tags:          map[string]string{"service": "bigquery", "action": "routines.list"},
		},
		HydrateConfig: []plugin.HydrateConfig{
			{
				Func: getBigQueryRoutine,
				Tags: map[string]string{"service": "bigquery", "action": "routines.get"},
			},
		},
		Columns: []*plugin.Column{
			{
				Name:        "routine_id",
				Description: "The ID of the routine.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RoutineReference.RoutineId"),
			},
			{
				Name:        "dataset_id",
				Description: "The ID of the dataset containing this routine.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RoutineReference.DatasetId"),
			},
			{
				Name:        "routine_type",
				Description: "The type of routine. Possible values are: SCALAR_FUNCTION, PROCEDURE, TABLE_VALUED_FUNCTION, AGGREGATE_FUNCTION.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "language",
				Description: "The language of the routine. Possible values are: SQL, JAVASCRIPT, PYTHON, JAVA, SCALA.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "creation_time",
				Description: "The time when this routine was created, in milliseconds since the epoch.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreationTime").Transform(transform.UnixMsToTimestamp),
			},
			{
				Name:        "last_modified_time",
				Description: "The time when this routine was last modified, in milliseconds since the epoch.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("LastModifiedTime").Transform(transform.UnixMsToTimestamp),
			},
			{
				Name:        "etag",
				Description: "A hash of this resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "The description of the routine, if defined.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "definition_body",
				Description: "The body of the routine.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "determinism_level",
				Description: "The determinism level of the JavaScript UDF. Possible values are: DETERMINISTIC, NOT_DETERMINISTIC.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "data_governance_type",
				Description: "If set to DATA_MASKING, the function is validated and made available as a masking function.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "security_mode",
				Description: "The security mode of the routine. Possible values are: DEFINER, INVOKER.",
				Type:        proto.ColumnType_STRING,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "strict_mode",
				Description: "If true, the procedure body is further checked for errors such as non-existent tables or columns.",
				Type:        proto.ColumnType_BOOL,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "imported_libraries",
				Description: "The paths of imported JavaScript libraries, for JavaScript routines.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "arguments",
				Description: "The arguments of the routine.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "return_type",
				Description: "The return type of the routine.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "return_table_type",
				Description: "The return table type for table-valued functions.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getBigQueryRoutine,
			},
			{
				Name:        "remote_function_options",
				Description: "Remote function specific options.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "spark_options",
				Description: "Spark specific options.",
				Type:        proto.ColumnType_JSON,
				Hydrate:     getBigQueryRoutine,
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RoutineReference.RoutineId"),
			},
			{
				Name:        "akas",
				Description: ColumnDescriptionAkas,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.From(bigQueryRoutineAkas),
			},

			// GCP standard columns
			{
				Name:        "project",
				Description: ColumnDescriptionProject,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RoutineReference.ProjectId"),
			},
		},
	}
}

//// LIST FUNCTION

func listBigQueryRoutines(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("listBigQueryRoutines")

	dataset := h.Item.(*bigquery.DatasetListDatasets)

	service, err := BigQueryService(ctx, d)
	if err != nil {
		return nil, err
	}

	pageSize := types.Int64(1000)
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

	resp := service.Routines.List(project, dataset.DatasetReference.DatasetId).MaxResults(*pageSize)
	if err := resp.Pages(ctx, func(page *bigquery.ListRoutinesResponse) error {
		d.WaitForListRateLimit(ctx)

		for _, routine := range page.Routines {
			d.StreamListItem(ctx, routine)

			if d.RowsRemaining(ctx) == 0 {
				page.NextPageToken = ""
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getBigQueryRoutine(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	plugin.Logger(ctx).Trace("getBigQueryRoutine")

	service, err := BigQueryService(ctx, d)
	if err != nil {
		return nil, err
	}

	projectId, err := getProject(ctx, d, h)
	if err != nil {
		return nil, err
	}
	project := projectId.(string)

	var datasetID, routineID string
	if h.Item != nil {
		data := h.Item.(*bigquery.Routine)
		datasetID = data.RoutineReference.DatasetId
		routineID = data.RoutineReference.RoutineId
	} else {
		datasetID = d.EqualsQuals["dataset_id"].GetStringValue()
		routineID = d.EqualsQuals["routine_id"].GetStringValue()
	}

	if routineID == "" || datasetID == "" {
		return nil, nil
	}

	resp, err := service.Routines.Get(project, datasetID, routineID).Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

//// TRANSFORM FUNCTIONS

func bigQueryRoutineAkas(_ context.Context, h *transform.TransformData) (interface{}, error) {
	data := h.HydrateItem.(*bigquery.Routine)

	projectID := data.RoutineReference.ProjectId
	datasetID := data.RoutineReference.DatasetId
	routineID := data.RoutineReference.RoutineId

	akas := []string{"gcp://bigquery.googleapis.com/projects/" + projectID + "/datasets/" + datasetID + "/routines/" + routineID}
	return akas, nil
}

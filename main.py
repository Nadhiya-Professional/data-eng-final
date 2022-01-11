import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class Transform_cust_tier_code(beam.DoFn):
    def process(self,element):
        element["CUST_TIER_CODE"] = str(element["CUST_TIER_CODE"])
        yield element

class Transform_order_total(beam.DoFn):
    def process(self,element):
        element["CUST_TIER_CODE"] = str(element["CUST_TIER_CODE"])
        element["TOTAL_SALES_AMOUNT"] = round(element["TOTAL_SALES_AMOUNT"],2)
        yield element


if __name__ == '__main__':

    table_spec_product = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_nadhiya_mathialagan',
        tableId='cust_tier_code-sku-total_no_of_product_views')
    table_schema = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'string', 'mode': 'Required'},
            {'name': 'sku', 'type': 'Integer', 'mode': 'Required'},
            {'name': 'total_no_of_product_views', 'type': 'Integer', 'mode': 'Required'}
            ]
    }

    table_spec_sales = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='final_nadhiya_mathialagan',
        tableId='cust_tier_code-sku-total_sales_amount')
    table_schema_sales = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'string', 'mode': 'Required'},
            {'name': 'sku', 'type': 'Integer', 'mode': 'Required'},
            {'name': 'total_sales_amount', 'type': 'float', 'mode': 'Required'}
        ]
    }

    pipeline_options = PipelineOptions(region = "us-central1", runner = "DataflowRunner", temp_location = "gs://york_temp_files",
                                       project = "york-cdf-start", job_name = "dataflow-nadhiya-final-new",
                                       staging_location = "gs://york_temp_files/staging")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        output = pipeline | "Read from customer and product view tables" >> beam.io.ReadFromBigQuery(query=""" select cust.CUST_TIER_CODE,product.SKU, count(*) as total_no_of_product_views from `york-cdf-start.final_input_data.customers` cust 
                                                                                    join `york-cdf-start.final_input_data.product_views` product 
                                                                                    on product.CUSTOMER_ID = cust.CUSTOMER_ID
                                                                                    group by cust.CUST_TIER_CODE,product.SKU""",
                                                                                    project ="york-cdf-start",use_standard_sql=True)

        cust_tier_transformed_output = output | beam.ParDo(Transform_cust_tier_code())

        cust_tier_transformed_output | "Write to bigquery first table" >> beam.io.WriteToBigQuery(
            table_spec_product,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        output_1 = pipeline | "Read from customer and orders tables" >> beam.io.ReadFromBigQuery(query="""select cust.CUST_TIER_CODE,orders.SKU,sum(orders.ORDER_AMT) as TOTAL_SALES_AMOUNT from `york-cdf-start.final_input_data.customers` cust 
                                                                                            join `york-cdf-start.final_input_data.orders` orders
                                                                                            on cust.CUSTOMER_ID = orders.CUSTOMER_ID
                                                                                            group by cust.CUST_TIER_CODE,orders.SKU""",
                                                                                            project="york-cdf-start",
                                                                                            use_standard_sql=True)

        total_sales_transformed_output = output_1 | beam.ParDo(Transform_order_total())

        total_sales_transformed_output | "Write to bigquery second table" >> beam.io.WriteToBigQuery(
            table_spec_sales,
            schema=table_schema_sales,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

        print("DONE1")
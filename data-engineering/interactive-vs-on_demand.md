# AWS EMR + EC2 Cost Usage Optimization (FinOps Playbook)

## Where EC2 Usage Cost Appears in CUR

In AWS Cost & Usage Reports (CUR), EMR compute cost is primarily visible
under EC2.

Key columns: - product_product_name - line_item_usage_type -
line_item_operation - line_item_resource_id - pricing_term -
line_item_unblended_cost

Filter example: product_product_name = Amazon Elastic Compute Cloud
line_item_usage_type LIKE 'BoxUsage%'

------------------------------------------------------------------------

## How EMR Costs Appear in CUR

Layer A --- EMR Service Fee\
product_product_name = Amazon Elastic MapReduce

Layer B --- EC2 Backing Compute\
product_product_name = Amazon Elastic Compute Cloud\
line_item_operation = RunInstances

Total EMR Cost = EMR Service Fee + EC2 + EBS

------------------------------------------------------------------------

## Tie EC2 Spend Back to EMR (Using Resource Tags)

Recommended Tags: - aws:elasticmapreduce:job-flow-id - Environment -
Application - CostCenter - Owner - WorkloadType (Interactive / Batch)

### Introducing Tagging

Option A --- Tag at cluster creation Option B --- Enforce via IAM policy
Option C --- Use Service Control Policies Option D --- Ensure
propagation to EC2 instance groups

------------------------------------------------------------------------

## Monitoring After Tagging

Option 1 --- AWS Cost Explorer (basic trend view)

Option 2 --- Athena on CUR (recommended) - Full SQL access - Per cluster
cost analysis - Spot vs OnDemand breakdown

Example query:

SELECT resource_tags_user_workloadtype, SUM(line_item_unblended_cost) AS
cost FROM cur_table WHERE product_product_name = 'Amazon Elastic Compute
Cloud' GROUP BY 1;

Option 3 --- QuickSight Dashboard Architecture: CUR (S3) → Athena →
QuickSight

------------------------------------------------------------------------

## Pattern A --- Long-Running Interactive Clusters

Symptoms: - 24/7 runtime - Low CPU utilization - High idle memory -
Activity only during business hours

Optimization: - Enable auto-termination - Reduce core node count - Use
Spot for task nodes - Split interactive vs job clusters

------------------------------------------------------------------------

## Compare OnDemand vs Spot

Check: - pricing_term - SpotUsage vs BoxUsage

Opportunity: Increase Spot adoption for batch-heavy workloads.

------------------------------------------------------------------------

## Right-Size Instance Types

Compute-heavy → c5\
Memory-heavy → r5\
Mixed → m5

Analyze CPU, memory, shuffle metrics.

------------------------------------------------------------------------

## Advanced Cost Patterns

-   Cost per TB processed
-   Cost per cluster (group by job-flow-id)
-   Idle executor cost
-   Runtime heatmap

------------------------------------------------------------------------

## FinOps Optimization Roadmap

Highest ROI: 1. Kill idle interactive clusters 2. Enable
auto-termination 3. Introduce Spot nodes 4. Enforce tagging

Medium ROI: 5. Right-size instances 6. Savings Plans 7. Executor tuning

Strategic: 8. EMR Serverless 9. EMR on EKS 10. Databricks evaluation

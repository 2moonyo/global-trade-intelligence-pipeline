select *
from {{ ref('mart_dataset_coverage_summary') }}
where (row_presence_score is not null and (row_presence_score < 0 or row_presence_score > 1))
   or (freshness_score is not null and (freshness_score < 0 or freshness_score > 1))
   or (expected_scope_score is not null and (expected_scope_score < 0 or expected_scope_score > 1))
   or (coverage_score is not null and (coverage_score < 0 or coverage_score > 1))
   or (overall_coverage_score is not null and (overall_coverage_score < 0 or overall_coverage_score > 1))

-- Test that all emergency works have correct work_category
select
    permit_reference_number,
    work_category,
    'Invalid work_category for emergency works' as failure_reason
from {{ ref('emergency_works_overview_england') }}
where work_category not in ('Immediate - emergency', 'Immediate - urgent') 
select count(distinct rfm_segment) as segment_count
from {{ ref('mrt__rfm_scoring') }}
having count(distinct rfm_segment) < 5
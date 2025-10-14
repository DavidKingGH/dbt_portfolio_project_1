with scored as (
  select
    user_id, 
    recency::int as recency, 
    frequency::int as frequency, 
    monetary::decimal as monetary,
    as_of_date::date as as_of_date, 
    ntile(5) over (partition by as_of_date order by recency asc)::int as   r_score,
    ntile(5) over (partition by as_of_date order by frequency desc)::int as f_score,
    ntile(5) over (partition by as_of_date order by monetary  desc)::int as m_score
  from {{ ref('mrt__rfm')}}
)
select
  *,
  case
    when r_score>=4 and f_score>=4 and m_score>=4 then 'champions'
    when m_score=5  and r_score>=3                then 'whales'
    when r_score>=4 and f_score>=4                then 'loyalists'
    when r_score>=4 and f_score=1 and m_score>=4  then 'big-ticket newbies'
    when r_score>=4 and f_score between 2 and 3   then 'promising'
    when f_score=5  and m_score<=2                then 'discount-churners'
    when r_score=3  and f_score<=2                then 'needs attention'
    when r_score<=2 and m_score>=4                then 'at-risk big spenders'
    when r_score<=2 and f_score<=2 and m_score<=2 then 'hibernating'
    when r_score=5  and f_score=1 and m_score<=3  then 'new'
    else 'other'
  end as rfm_segment
from scored
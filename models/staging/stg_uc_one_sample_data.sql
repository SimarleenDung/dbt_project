select
    lower(email) as email,
    first_name,
    middle_name,
    last_name
from {{ ref('uc_one_sample_data') }}

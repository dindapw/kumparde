select id,
       title,
       content,
       published_at,
       author_id,
       created_at,
       updated_at,
       deleted_at
from kumparde.articles
where 1=1
$date_condition
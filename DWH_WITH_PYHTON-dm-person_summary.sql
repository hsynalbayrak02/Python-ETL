insert into dm.person_summary
select
cast(getdate() as date) snapshot_date,
PersonType,
EmailPromotion,
AVG(LEN(FirstName)) avg_name_length,
getdate() load_date
from dbo.[Person]
group by
PersonType,
EmailPromotion
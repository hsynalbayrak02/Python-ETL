insert into  dm.currency_sumary
select CurrencyCode,COUNT(*) Adet,getdate()
from [dbo].[Currency]
Group By CurrencyCode
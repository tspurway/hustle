

select(i.token, min(i.time), where=((i.campaign_io_id == 17146) |(i.campaign_io_id == 17147) |(i.campaign_io_id == 17160)) & (i.impressions > 0) & (i.is_psa == 0) & (i.date == '2014-02-20'), nest=True)


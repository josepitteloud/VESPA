select
		1_ResolutionValue
	,	cast(substr(1_ResolutionValue,1,locate('X',1_ResolutionValue)-1) as int)	pixels_wide
	,	substr(device_id,1,3)														nds_pref
	,	count(distinct device_id)													devices
from	(
			select
					device_id
				,	dt
				,	1_ResolutionValue
				,	rank() over	(
									partition by	device_id
									order by		dt desc
								)	rnk
			from	(
						select
							device_id
							,	report_timestamp
							,	cast(concat('20',substr(report_timestamp,7,2),'-',substr(report_timestamp,4,2),'-',substr(report_timestamp,1,2),' ',substr(report_timestamp,10,12)) as timestamp) dt
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.Name' then parameter_value else NULL end) as 1_Name
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.Status' then parameter_value else NULL end) as 1_Status
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.Status' then parameter_value else NULL end) as 1_DisplayDevice_Status
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.VideoLatency' then parameter_value else NULL end) as 1_DisplayDevice_VideoLatency
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.CECSupport' then parameter_value else NULL end) as 1_DisplayDevice_CECSupport
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.PreferredResolution' then parameter_value else NULL end) as 1_DisplayDevice_PreferredResolution
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.SupportedResolutions' then parameter_value else NULL end) as 1_DisplayDevice_SupportedResolutions
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.AutoLipSyncSupport' then parameter_value else NULL end) as 1_DisplayDevice_AutoLipSyncSupport
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.ResolutionMode' then parameter_value else NULL end) as 1_ResolutionMode
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.EEDID' then parameter_value else NULL end) as 1_DisplayDevice_EEDID
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.HDMI3DPresent' then parameter_value else NULL end) as 1_DisplayDevice_HDMI3DPresent
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.ResolutionValue' then parameter_value else NULL end) as 1_ResolutionValue
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.DisplayDevice.Name' then parameter_value else NULL end) as 1_DisplayDevice_Name
							,	max(case parameter_name when 'Device.Services.STBService.Components.HDMI.1.Enable' then parameter_value else NULL end) as 1_Enable
						from	pace
						where
								device_id						like	'32%'
							and	parameter_name					like	'Device.Services.STBService.Components.HDMI.%'
							and	cast(d as int)					>=		20160601 -- between 20160601 and 20160701
							and	substr(report_timestamp,22,1)	=		'Z'
						group by
								device_id
							,	report_timestamp
							,	dt
						having
								1_status								=		'Enabled'
							and	1_DisplayDevice_Status					=		'Started'
						--	and	1_DisplayDevice_SupportedResolutions	like	'%3840%'
						--order by
						--		device_id
						--	,	report_timestamp
						--	,	dt
						--limit 100
					)	t0
		)	t1
where	rnk	=	1
group by
		1_ResolutionValue
	,	pixels_wide
	,	nds_pref
order by
		pixels_wide
	,	1_ResolutionValue
	,	nds_pref
;
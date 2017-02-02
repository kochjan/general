import sys 

PRODUCTION_ACTIVE = 1 

PM_ALERTS = ['pm-alerts@nipuncapital.com']
DATA_ALERTS = ['data-alerts@nipuncapital.com']


ftp_log_table = 'production_reporting.dbo.ftp_log'
file_log_table = 'production_reporting.dbo.file_log'
report_schema = 'production_reporting'
task_schema = 'production_task '
temp_db_schema = 'production_temp'
primary_db = 'nipun_prod'
primary_outputs = 'outputs/'

### DIRECTORY STRUCTURE
production_root = '/production/'
production_current = '%s/current/' % production_root
production_dir = production_root +'/%(YYYY)s/%(MM)s/%(YYYYMMDD)s/'
daily_alpha_dir = '%s/daily_alphas/' % production_root
debug_dir = '%s/debug/' % production_dir
pb_dir = '%s/pb/' % production_current
pre_alpha_dir = '%s/prealpha/' % production_current
prealp_dir = '%s/prealpha/' % production_dir
alpha_dir = '%s/alphas/' % production_dir
alphagen_dir = '%s/alphagen/' % production_dir
optimizer_dir = '%s/optimizer/' % production_dir
report_dir = '%s/reports/' % production_dir
univ_dir = '%s/universe/' % production_dir
axioma_dir = '%s/axioma/' % production_dir
holdings_dir = '%s/holdings/' % production_dir
current_holdings_dir = '%s/holdings/' % production_current

derived_dir = '/opt/data/nipun_derived/'
alpha_signal_dir = 'production_signals/' 
h5_archive_location = '/opt/data/backups/h5/'
h5_fast_archive = '/dev/shm/backups/h5/'

gics_current = '%s.dbo.gics_current' % primary_db
gics_history_table = '%s.dbo.gics_history' % primary_db
axioma_history_table = '%s.dbo.axioma_adv' % primary_db
axioma_info_table = '%s.dbo.axioma_info' % primary_db
axioma_predicted_beta = '%s.dbo.axioma_pred_beta' % primary_db
### SECURITY
security_master_table = '%s.dbo.security_master' % primary_db
security_output_path = primary_outputs

## DERIVED
iso_country_map = '/opt/data/nipun_derived/static/iso_country.csv'
barra_pickle_loc = '/opt/data/nipun_derived/static/barra_exch_info.pck'
barra_holiday_file = '/opt/data/nipun_derived/static/Holidays.GL'

## WORLDSCOPE
worldscope_current_schema = 'production_fundamentals' #primary_db
worldscope_historical_schema = 'production_fundamentals' #worldscope_current_schema
worldscope_current_table = 'dbo.ws_pit_av__current_%s'
worldscope_historical_table = 'dbo.ws_pit_av__%s'

worldscope_custom_fields = 'nipun_task/worldscope/custom_fields.csv'
worldscope_output_dir = primary_outputs

### IBES

ibes_schema = 'qai'
ibes_database = 'qai'
ibes_universes = ['ibg', 'ibe']

ibes_split_table = '%s.dbo.ibg_splits' % ibes_schema
ibg_cumsplit_table = '%s.dbo.ibg_cumsplit' % ibes_schema
ibes_estimate_period = '%s.dbo.estimate_period_new' % ibes_schema
ibes_estimate_table = '%s.dbo.est_clean_ibg' % ibes_schema
ibes_consensus_table = '%s.dbo.nipun_consensus' % ibes_schema

ibes_parent_override = '%s.dbo.parent_override' % ibes_schema

ibes_cache_path = '/opt/data/ibes/estimates/'
ibg_cache_split = 'ibg_splits.h5'
ibg_cache_fpe = 'ibg_fpe.h5'

ibes_output_path = primary_outputs
consensus_output_path = primary_outputs

## TOYO ESTIMATES
db_connection='qai'

toyo_schema = primary_db
toyo_database = db_connection
toyo_split_table = '%s.dbo.tk_splits' % toyo_schema
toyo_estimate_table = '%s.dbo.tk_est_clean' % toyo_schema
toyo_output_path = primary_outputs


### ESTIMATES
estimate_period = '%s.dbo.est_per_view' % primary_db
estimate_table = '%s.dbo.est_clean' % primary_db

barra_universe_path = univ_dir
barra_output_path = optimizer_dir + '/%(univ)s/'

barra_returns_path = '/opt/data/barra/returns/'
barra_daily_cache = [('GEM2','S')]
barra_monthly_cache = [('GEM2','S'), ('ASE1JPN','S')]

### DS2
ds2_data_path = '/opt/data/datastream/'
ds2_volume_file = 'volume.h5'

### REPORTING PATHS
cpa_cache_files = '/opt/data/nipun_derived/static/'

asx_shortvol_table = 'nipun_prod.dbo.asx_short_volume'
asic_shortint_table = 'nipun_prod.dbo.asic_short_interest'
hkg_shortvol_table = 'nipun_prod.dbo.hkg_short_volume'
hkg_short_int_table = 'nipun_prod.dbo.hkg_short_interest'
hkg_warrant_table = 'nipun_prod.dbo.hkg_warrants'

gretai_short_table = 'nipun_prod.dbo.tw_gretai_short'
twse_si_table = 'production_twse.dbo.TWT93U'

short_avail_table = 'nipun_prod.dbo.short_availability_data'
short_summary_table = 'nipun_prod.dbo.short_availability_current'


## reuters ftp
reuters_output_path = primary_outputs
sig_dev_outputs = primary_outputs
repno_mapping_table = 'nipun_prod.dbo.tr_repno_mapping'
ric_table = 'nipun_prod.dbo.ric_map'
sigdev_table = 'nipun_prod.dbo.sigdev_data'

## wisefn
wisefn_output_path = primary_outputs
wisefn_co_data = 'production_wise.dbo.wisefn_company_data'
wisefn_findata = 'production_wise.dbo.wisefn_findata_ifrs'
wisefn_item_tbl = 'production_wise.dbo.wisefn_item_desc'
wisefn_field_def_tbl = 'production_wise.dbo.wisefn_field_def'
wisefn_codes = 'production_wise.dbo.wisefn_code_desc'
wisefn_finprd_tbl = 'production_wise.dbo.wisefn_finprd'

## KRX tables
krx_inv_table = 'nipun_prod.dbo.krx_inv_type'
twn_insider_data = 'nipun_prod.dbo.taiwan_insider_data'
twn_otc_insider_data = 'nipun_prod.dbo.taiwan_otc_insider_data'

## directors deals
directors_deals_data = 'nipun_prod.dbo.directors_deals'

## THAI TABLES
thai_insider_data = 'nipun_prod.dbo.thai_insider'
thai_name_map = 'nipun_prod.dbo.thai_name_map'

## MYS INSIDER
mys_insider_data = 'nipun_prod.dbo.mys_insider_data'

### SGX TABLES
sgx_sv_table = 'nipun_prod.dbo.sgx_short_volume'

### TSV TABLES
e12p_tsv_table = 'nipun_prod.dbo.e12p_tsv_data'
d12p_tsv_table = 'nipun_prod.dbo.d12p_tsv_data'
s12ev_tsv_table = 'nipun_prod.dbo.s12ev_tsv_data'

#JPN TABLES
jsda_balance = 'nipun_prod.dbo.jsda_balance'
jsda_new_borrow = 'nipun_prod.dbo.jsda_new_borrow'

india_futures_table = 'nipun_prod.dbo.india_futures'
india_delivery_table = 'nipun_prod.dbo.india_delivery'
india_futures_pricing = 'nipun_prod.dbo.india_futures_prices'


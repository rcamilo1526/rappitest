
snowsql -a la13042.us-east-2.aws
use meetup
create stage meetup_stage file_format=csv_tab;
put file:///mnt/e/prueba_der/data/output/members*.csv @meetup_stage auto_compress=true;

copy into members_topics
  from @my_csv_stage
  file_format = (format_name = csv_tab)
  pattern='.*contacts[1-5].csv.gz'
  on_error = 'skip_file';

copy into members
  from @meetup_stage/members.csv.gz
  file_format = (format_name = csv_tab)
  on_error = 'skip_file';
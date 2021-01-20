"""
Generate Table 5 Individual vs General Summary Statistics in SNC Distributive Impacts Paper: https://www.overleaf.com/project/5eed55fb7ff4580001737f38
"""
import os

import git
import numpy as np
import pandas as pd
from scipy import stats
import sqlalchemy as sa

DB_URI = os.getenv('EPA_REGVCLASS_DWH')
ENGINE = sa.create_engine(DB_URI)


QUERY = """
-- From https://app.mode.com/editor/reglab/reports/490b375c6a60
with qncr as 
(
/*
This is unique at the npdes_permit_id level
*/
select 
  sub2.npdes_permit_id,
  ref_hlrnc.hlrnc as highest_qtr_status,
  sub2.highest_qtr_status_numeric,
  case when ref_hlrnc.hlrnc in ('S', 'E', 'X', 'T', 'D') then 1 else 0 end as snc_flag, -- https://echo.epa.gov/help/reports/dfr-data-dictionary; search for 'The following are NPDES Quarterly Noncompliance Report (QNCR) codes that translate to SNC status (from most to least important):''
  case when ref_hlrnc.hlrnc in ('E', 'X') then 1 else 0 end as esnc_flag,
  case when ref_hlrnc.hlrnc in ('D') then 1 else 0 end as dmrnr_flag
from 
(
  select 
    npdes_permit_id,
    coalesce(max(hlrnc_value_numeric), 0) as highest_qtr_status_numeric
  from 
    (
      select 
        qncr.npdes_id as npdes_permit_id,
        qncr.yearqtr as fiscal_quarter,
        qncr.hlrnc,
        -- Hierarchy the same as EPA's implementation (erule_ready.sql in EPA Production Git Repository) 
        ref_hlrnc.hlrnc_value_numeric,
        count(*) over (partition by npdes_id) as number_quarters_rnc_tracking_on
      from data_ingest.icis__npdes_qncr_history as qncr
        left join ontologies.icis__ref_hlrnc_code as ref_hlrnc
          on qncr.hlrnc = ref_hlrnc.hlrnc
      where qncr.file_timestamp = '2019-12-23'
        and qncr.yearqtr in ('20181', '20182', '20183', '20184')
    ) as sub 
  where sub.number_quarters_rnc_tracking_on = 4 -- Needs to be on for all 4 quarters
  group by sub.npdes_permit_id
) as sub2
  left join ontologies.icis__ref_hlrnc_code as ref_hlrnc
    -- Because both U and W tie to 0 so if we were to just join you get duplicates.
    on case when sub2.highest_qtr_status_numeric = 0 then null else sub2.highest_qtr_status_numeric end = ref_hlrnc.hlrnc_value_numeric
),

permits as 
(
/*
This is unique at the npdes_permit_id level

Filters to only Individual and General Permits
*/
select 
  npdes_permit_id,
  individual_permit_flag,
  general_permit_flag,
  original_effective_date,
  coalesce(termination_date, '2049-09-30'::date) as termination_date -- 2049-09-30 is the coalesced termination date that EPA uses in erule_ready.sql
from icis.permits as permits 
where permits.latest_version_flag 
  and (individual_permit_flag = 1 or general_permit_flag = 1)
  and original_effective_date is not null -- About 1.5% of rows are missing original effective date so not a large number. I also think the official EPA script (erule_ready.sql) filters these out anyway
),

snc_query as 
(
select 
  qncr.npdes_permit_id,
  qncr.highest_qtr_status,
  qncr.snc_flag,
  qncr.esnc_flag,
  qncr.dmrnr_flag,
  permits.individual_permit_flag,
  permits.general_permit_flag
from qncr
  inner join permits 
    on qncr.npdes_permit_id = permits.npdes_permit_id 
where permits.original_effective_date <= '2017-10-01'  -- Beginning of 2018 Q1 Fiscal Year 
  and permits.termination_date > '2018-09-30' -- End of 2018 Q4 Fiscal Year
),

echo as 
(
/*
I've checked and this is unique at the npdes_permit_id level
*/
select 
  unnest(string_to_array(npdes_ids, ' ')) as npdes_permit_id,
  fac_pop_den,
  ejscreen_flag_us,
  fac_percent_minority
from data_ingest.echo__facilities
where npdes_ids is not null
)

select 
  snc_query.npdes_permit_id,
  snc_query.highest_qtr_status,
  snc_query.snc_flag,
  snc_query.esnc_flag,
  snc_query.dmrnr_flag,
  snc_query.individual_permit_flag,
  snc_query.general_permit_flag,
  echo.fac_pop_den,
  echo.ejscreen_flag_us,
  echo.fac_percent_minority,
  case when census.median_hh_income < 0 then null else census.median_hh_income end as median_hh_income,
  permits.wastewater_permit_flag,
  permits.stormwater_permit_flag,
  case when permits.major_minor_status_flag = 'M' then 1 else 0 end as major_permit_flag
from snc_query
  left join echo
    on snc_query.npdes_permit_id = echo.npdes_permit_id
  left join icis.facilities as facilities 
    on snc_query.npdes_permit_id = facilities.npdes_permit_id
  left join api.census__acs5_block_group as census
    on facilities.census_block_group_id = census.census_block_group_id
  left join icis.permits as permits 
    on snc_query.npdes_permit_id = permits.npdes_permit_id 
      and permits.latest_version_flag
"""

LATEX_TEMPLATE = r"""
\begin{{table}}[htb] \centering 
\begin{{tabular}}{{\extracolsep{{0pt}}cccc}}
%\\[-1.8ex]\hline 
\hline %\\[-1.8ex] 
 & \textbf{{General}} & \textbf{{Individual}} & \textbf{{P-Value}}* \\ 
\hline %\\[-1.8ex] 
\multicolumn{{1}}{{l}}{{Number of Facilities}} & {general_total_count} & {individual_total_count} &  \\ 
&&& \\
\multicolumn{{1}}{{l}}{{Effluent SNC (Rate)}} & {general_eff_snc_count} ({general_eff_snc_rate}\%) & {individual_eff_snc_count} ({individual_eff_snc_rate}\%) &  \\ 
&&& \\
\multicolumn{{1}}{{l}}{{\textbf{{Facility Features**}}}} & & & \\
\multicolumn{{1}}{{r}}{{Wastewater (Rate)}} & {general_ww_count} ({general_ww_rate}\%) & {individual_ww_count} ({individual_ww_rate}\%) &  \\
\multicolumn{{1}}{{r}}{{Stormwater (Rate)}} & {general_sw_count} ({general_sw_rate}\%)  & {individual_sw_count} ({individual_sw_rate}\%) &   \\ 
\multicolumn{{1}}{{r}}{{Major (Rate)}} & {general_major_count} ({general_major_rate}\%) & {individual_major_count} ({individual_major_rate}\%) &  \\ 
&&& \\
\multicolumn{{1}}{{l}}{{\textbf{{Demographic}}}} &  & & \\
\multicolumn{{1}}{{l}}{{\textbf{{Features}}}} &  & & \\
\multicolumn{{1}}{{r}}{{Avg Population}} & {general_avg_population_density} & {individual_avg_population_density} & {avg_population_density_p_value} \\
\multicolumn{{1}}{{r}}{{Density}} & & & \\
\multicolumn{{1}}{{r}}{{Avg Median}} & {general_avg_median_hh_income} & {individual_avg_median_hh_income} & {avg_median_hh_income_p_value} \\
\multicolumn{{1}}{{r}}{{Household Income}} &  &  & \\
\multicolumn{{1}}{{r}}{{Avg Percentage}} & {general_avg_percentage_minority}\% & {individual_avg_percentage_minority}\% & {avg_percentage_minority_p_value}\\
\multicolumn{{1}}{{r}}{{Minority}} & & & \\
\hline %\\[-1.8ex] 
\end{{tabular}} 
  \caption{{Characteristics of Individual and General Permits. \\ *P-Values from Welch's two Sample T-test of means. \\**Non-exclusive categories.}} 
  \label{{tab:individual_vs_general}} 
 \vspace{{-0.5em}}% Remove excess white space after figure

\end{{table}} 
"""


def get_git_root(path):
    """Return Top Level Git Repository directory given path"""
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return git_root


def main():
    git_root_dir = get_git_root(os.path.dirname(__file__))
    with ENGINE.begin() as conn:
        df = pd.read_sql(QUERY, conn)
        df_individual = df.loc[df['individual_permit_flag'] == 1].copy(deep=True)
        df_general = df.loc[df['general_permit_flag'] == 1].copy(deep=True)

    general_total_count = df_general.shape[0]
    individual_total_count = df_individual.shape[0]
    general_eff_snc_count = df_general.esnc_flag.sum()
    individual_eff_snc_count = df_individual.esnc_flag.sum()
    general_eff_snc_rate = int(round(general_eff_snc_count / general_total_count * 100))
    individual_eff_snc_rate = int(round(individual_eff_snc_count / individual_total_count * 100))
    general_ww_count = df_general.wastewater_permit_flag.sum()
    general_ww_rate = int(round(general_ww_count / general_total_count * 100))
    individual_ww_count = df_individual.wastewater_permit_flag.sum()
    individual_ww_rate = int(round(individual_ww_count / individual_total_count * 100))
    general_sw_count = df_general.stormwater_permit_flag.sum()
    general_sw_rate = int(round(general_sw_count / general_total_count * 100))
    individual_sw_count = df_individual.stormwater_permit_flag.sum()
    individual_sw_rate = int(round(individual_sw_count / individual_total_count * 100))
    general_major_count = df_general.major_permit_flag.sum()
    general_major_rate = int(round(general_major_count / general_total_count * 100))
    individual_major_count = df_individual.major_permit_flag.sum()
    individual_major_rate = int(round(individual_major_count / individual_total_count * 100))
    general_avg_population_density = int(round(df_general.fac_pop_den.mean()))
    individual_avg_population_density = int(round(df_individual.fac_pop_den.mean()))
    avg_population_density_p_value = round(stats.ttest_ind(
            df_individual.fac_pop_den.dropna(),
            df_general.fac_pop_den.dropna(),
            equal_var=False
        ).pvalue, 2)
    general_avg_median_hh_income = int(round(df_general.median_hh_income.mean()))
    individual_avg_median_hh_income = int(round(df_individual.median_hh_income.mean()))
    avg_median_hh_income_p_value = round(stats.ttest_ind(
        df_individual.median_hh_income.dropna(),
        df_general.median_hh_income.dropna(),
        equal_var=False
    ).pvalue, 2)
    general_avg_percentage_minority = int(round(df_general.fac_percent_minority.mean()))
    individual_avg_percentage_minority = int(round(df_individual.fac_percent_minority.mean()))
    avg_percentage_minority_p_value = round(stats.ttest_ind(
        df_individual.fac_percent_minority.dropna(),
        df_general.fac_percent_minority.dropna(),
        equal_var=False
    ).pvalue, 2)

    # # Note, '{:,}}' formats number with thousand comma separator
    info = {
        'general_total_count': '{:,}'.format(general_total_count),
        'individual_total_count': '{:,}'.format(individual_total_count),
        'general_eff_snc_count': '{:,}'.format(general_eff_snc_count),
        'general_eff_snc_rate': '{:,}'.format(general_eff_snc_rate),
        'individual_eff_snc_count': '{:,}'.format(individual_eff_snc_count),
        'individual_eff_snc_rate': '{:,}'.format(individual_eff_snc_rate),
        'general_ww_count': '{:,}'.format(general_ww_count),
        'general_ww_rate': '{:,}'.format(general_ww_rate),
        'individual_ww_count': '{:,}'.format(individual_ww_count),
        'individual_ww_rate': '{:,}'.format(individual_ww_rate),
        'general_sw_count': '{:,}'.format(general_sw_count),
        'general_sw_rate': '{:,}'.format(general_sw_rate),
        'individual_sw_count': '{:,}'.format(individual_sw_count),
        'individual_sw_rate': '{:,}'.format(individual_sw_rate),
        'general_major_count': '{:,}'.format(general_major_count),
        'general_major_rate': '{:,}'.format(general_major_rate),
        'individual_major_count': '{:,}'.format(individual_major_count),
        'individual_major_rate': '{:,}'.format(individual_major_rate),
        'general_avg_population_density': '{:,}'.format(general_avg_population_density),
        'individual_avg_population_density': '{:,}'.format(individual_avg_population_density),
        'avg_population_density_p_value': '{:.2f}'.format(avg_population_density_p_value),
        'general_avg_median_hh_income': '{:,}'.format(general_avg_median_hh_income),
        'individual_avg_median_hh_income': '{:,}'.format(individual_avg_median_hh_income),
        'avg_median_hh_income_p_value': '{:.2f}'.format(avg_median_hh_income_p_value),
        'general_avg_percentage_minority': '{:,}'.format(general_avg_percentage_minority),
        'individual_avg_percentage_minority': '{:,}'.format(individual_avg_percentage_minority),
        'avg_percentage_minority_p_value': '{:.2f}'.format(avg_percentage_minority_p_value),
    }
    
    table5_latex = LATEX_TEMPLATE.format(**info)
    with open(os.path.join(git_root_dir, 'output', 'tables', 'table5_individual_vs_general.tex'), 'w') as f:
        f.write(table5_latex)


if __name__ == "__main__":
    main()
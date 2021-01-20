"""
Generate Table 1 Summary Statistics in SNC Distributive Impacts Paper: https://www.overleaf.com/project/5eed55fb7ff4580001737f38
"""
import os

import git
import numpy as np
import pandas as pd
import sqlalchemy as sa

DB_URI = os.getenv('EPA_REGVCLASS_DWH')
ENGINE = sa.create_engine(DB_URI)


QUERY = """
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
)

select 
  individual_permit_flag,
  count(*) as total_permits,
  sum(snc_flag) as total_snc,
  avg(snc_flag) as perc_snc,
  sum(esnc_flag) as total_esnc,
  avg(esnc_flag) as perc_esnc,
  sum(dmrnr_flag) as total_dmrnr,
  avg(dmrnr_flag) as perc_dmrnr
from snc_query
group by 1
"""

LATEX_TEMPLATE = r"""
\begin{{table}}[htb] \centering 
\begin{{tabular}}{{\extracolsep{{0pt}}rrr}}
    %\\[-1.8ex]\hline 
    \hline %\\[-1.8ex] 
    \textbf{{Status}} & \MC{{1}}{{c}}{{\textbf{{General (Rate)}}}} & \MC{{1}}{{c}}{{\textbf{{Individual (Rate)}}}} \\ 
    \hline %\\[-1.8ex] 
    \textbf{{Total SNC}} &\textbf{{{general_total_snc_count} ({general_total_snc_rate}\%)}} & \textbf{{{individual_total_snc_count} ({individual_total_snc_rate}\%)}} \\
        \textit{{DMR Non-Receipt SNC}} & {general_dmrnr_snc_count} ({general_dmrnr_snc_rate}\%) & {individual_dmrnr_snc_count} ({individual_dmrnr_snc_rate}\%) \\ 
        \textit{{Effluent SNC}} & {general_eff_snc_count} ({general_eff_snc_rate}\%) & {individual_eff_snc_count} ({individual_eff_snc_rate}\%) \\ 
        \textit{{Other SNC }}& {general_oth_snc_count} ({general_oth_snc_rate}\%) & {individual_oth_snc_count} ({individual_oth_snc_rate}\%) \\
    \textbf{{Non-SNC}} & \textbf{{{general_non_snc_count} ({general_non_snc_rate}\%)}}  & \textbf{{{individual_non_snc_count} ({individual_non_snc_rate}\%)}} \\ 
    \hline
    \textbf{{Total}} & \textbf{{{general_total_count}}} & \textbf{{{individual_total_count}}}\\
\hline \\[-1.8ex] 
\end{{tabular}} 
  \caption{{Types and Quantities of Significant Noncompliance among General and Individual Permittees}}
  %Discharge Monitoring Report (DMR) non-submission accounts for the majority of SNC, as indicated by this table that shows the compliance status for permits in FY2018. The category ``other SNC" most frequently refers to violations of an agreed upon compliance schedule. 
  
  %Note, while the CWA regulates over 400k facilities, we show only the much smaller subset of facilities actually included in the NCI calculation. Principally, the NCI calculation only includes individual permits which were active and whose compliance tracking was turned on for the entirety of 2018 FY.
  \label{{tab:compliance_status}}
  \vspace{{-8mm}}% Remove excess white space after the table 
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

    general_total_snc_count = df.loc[df.individual_permit_flag == 0].total_snc.iloc[0]
    general_total_snc_rate = int(round(df.loc[df.individual_permit_flag == 0].perc_snc.iloc[0] * 100))
    individual_total_snc_count = df.loc[df.individual_permit_flag == 1].total_snc.iloc[0]
    individual_total_snc_rate = int(round(df.loc[df.individual_permit_flag == 1].perc_snc.iloc[0] * 100))
    general_dmrnr_snc_count = df.loc[df.individual_permit_flag == 0].total_dmrnr.iloc[0]
    general_dmrnr_snc_rate = int(round(df.loc[df.individual_permit_flag == 0].perc_dmrnr.iloc[0] * 100))
    individual_dmrnr_snc_count = df.loc[df.individual_permit_flag == 1].total_dmrnr.iloc[0]
    individual_dmrnr_snc_rate = int(round(df.loc[df.individual_permit_flag == 1].perc_dmrnr.iloc[0] * 100))
    general_eff_snc_count = df.loc[df.individual_permit_flag == 0].total_esnc.iloc[0]
    general_eff_snc_rate = int(round(df.loc[df.individual_permit_flag == 0].perc_esnc.iloc[0] * 100))
    individual_eff_snc_count = df.loc[df.individual_permit_flag == 1].total_esnc.iloc[0]
    individual_eff_snc_rate = int(round(df.loc[df.individual_permit_flag == 1].perc_esnc.iloc[0] * 100))
    general_oth_snc_count = general_total_snc_count - general_dmrnr_snc_count - general_eff_snc_count
    individual_oth_snc_count = individual_total_snc_count - individual_dmrnr_snc_count - individual_eff_snc_count
    general_non_snc_count = df.loc[df.individual_permit_flag == 0].total_permits.iloc[0] - df.loc[df.individual_permit_flag == 0].total_snc.iloc[0]
    individual_non_snc_count = df.loc[df.individual_permit_flag == 1].total_permits.iloc[0] - df.loc[df.individual_permit_flag == 1].total_snc.iloc[0]
    general_total_count = df.loc[df.individual_permit_flag == 0].total_permits.iloc[0]
    individual_total_count = df.loc[df.individual_permit_flag == 1].total_permits.iloc[0]
    general_non_snc_rate = int(round(general_non_snc_count / general_total_count * 100))
    individual_non_snc_rate = int(round(individual_non_snc_count / individual_total_count * 100))
    general_oth_snc_rate = int(round(general_oth_snc_count / general_total_count * 100))
    individual_oth_snc_rate = int(round(individual_oth_snc_count / individual_total_count * 100))

    # Note, '{:,}' formats number with thousand comma separator
    info = {
        'general_total_snc_count': '{:,}'.format(general_total_snc_count),
        'general_total_snc_rate': '{:,}'.format(general_total_snc_rate),
        'individual_total_snc_count': '{:,}'.format(individual_total_snc_count),
        'individual_total_snc_rate': '{:,}'.format(individual_total_snc_rate),
        'general_dmrnr_snc_count': '{:,}'.format(general_dmrnr_snc_count),
        'general_dmrnr_snc_rate': '{:,}'.format(general_dmrnr_snc_rate),
        'individual_dmrnr_snc_count': '{:,}'.format(individual_dmrnr_snc_count),
        'individual_dmrnr_snc_rate': '{:,}'.format(individual_dmrnr_snc_rate),
        'general_eff_snc_count': '{:,}'.format(general_eff_snc_count),
        'general_eff_snc_rate': '{:,}'.format(general_eff_snc_rate),
        'individual_eff_snc_count': '{:,}'.format(individual_eff_snc_count),
        'individual_eff_snc_rate': '{:,}'.format(individual_eff_snc_rate),
        'general_oth_snc_count': '{:,}'.format(general_oth_snc_count),
        'general_oth_snc_rate': '{:,}'.format(general_oth_snc_rate),
        'individual_oth_snc_count': '{:,}'.format(individual_oth_snc_count),
        'individual_oth_snc_rate': '{:,}'.format(individual_oth_snc_rate),
        'general_non_snc_count': '{:,}'.format(general_non_snc_count),
        'general_non_snc_rate': '{:,}'.format(general_non_snc_rate),
        'individual_non_snc_count': '{:,}'.format(individual_non_snc_count),
        'individual_non_snc_rate': '{:,}'.format(individual_non_snc_rate),
        'general_total_count': '{:,}'.format(general_total_count),
        'individual_total_count': '{:,}'.format(individual_total_count),
    }
    
    table1_latex = LATEX_TEMPLATE.format(**info)
    with open(os.path.join(git_root_dir, 'output', 'tables', 'table1_snc_breakdown_2018.tex'), 'w') as f:
        f.write(table1_latex)


if __name__ == "__main__":
    main()
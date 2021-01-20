"""
Generate Figure 1, distribution of San Mateo HH Income in SNC Distributive Impacts Paper: https://www.overleaf.com/project/5eed55fb7ff4580001737f38
"""
from io import BytesIO
import os
import requests
import tempfile
from urllib.request import urlopen
from zipfile import ZipFile

from census import Census
import geopandas as gpd
import git
from matplotlib import gridspec
import matplotlib.pyplot as plt
import osr
import numpy as np
import pandas as pd
from scipy import stats
import seaborn as sns
import shapefile
from shapely.geometry import shape 
import sqlalchemy as sa
import us
from us import states

DB_URI = os.getenv('EPA_REGVCLASS_DWH')
ENGINE = sa.create_engine(DB_URI)

CENSUS_API_KEY = os.getenv('CENSUS_API')
EXAMPLE_COUNTY_FIPS = '081'  # 081: San Mateo County
EXAMPLE_STATE = 'CA'

ZIPPED_SHP_URL = 'https://www2.census.gov/geo/tiger/TIGER2019/BG/tl_2019_06_bg.zip'


ACS_QUERY = """
select 
  census_block_group_id,
  name,
  median_hh_income,
  population,
  nonhisp_white,
  ed_total_above25,
  ed_ba,
  ed_ma,
  ed_prof,
  ed_phd,
  state,
  county,
  tract,
  block_group,
  share_ba,
  share_phd,
  prop_non_hispanic_white,
  prop_nonwhite
from api.census__acs5_block_group
where state = '{}'
  and county = '{}'
""".format(eval('states.{}.fips'.format(EXAMPLE_STATE)), EXAMPLE_COUNTY_FIPS)

# Reading from PostgreSQL into Pandas DataFrame
EJ_QUERY = """
select
  id as census_block_group_id,
  pwdis,
  d_pwdis_2,
  p_pwdis,
  b_pwdis,
  t_pwdis
from data_ingest.ejscreen__census_block_group
"""

FACILITIES_QUERY = """
with snc_permits as (
/*
This is unique at the npdes_permit_id level
*/
select 
  npdes_permit_id,
  snc_flag,
  esnc_flag
from icis.npdes_latest_reported_qncr_history
where fiscal_quarter = '20201'
)

select 
  facilities.icis_facility_interest_id,
  facilities.facility_uin,
  facilities.npdes_permit_id,
  facilities.permit_state,
  facilities.location_address,
  facilities.supplemental_address_text,
  facilities.city,
  facilities.county_code,
  facilities.state_code,
  facilities.zip,
  facilities.census_block_group_id,
  facilities.geocode_latitude,
  facilities.geocode_longitude,
  permits.permit_status_code,
  permits.permit_status_code_desc,
  permits.is_currently_active_flag,
  permits.individual_permit_flag,
  snc_permits.snc_flag,
  snc_permits.esnc_flag
from icis.facilities as facilities
  left join icis.permits as permits 
    on facilities.npdes_permit_id = permits.npdes_permit_id 
      and permits.latest_version_flag 
  left join snc_permits 
    on facilities.npdes_permit_id = snc_permits.npdes_permit_id
where left(facilities.census_block_group_id, 5) = '{}{}'
  and (permits.individual_permit_flag = 1 or permits.general_permit_flag = 1)
--   and permits.sewage_permit_flag = 1
--   and permits.wastewater_permit_flag = 1
""".format(eval('states.{}.fips'.format(EXAMPLE_STATE)), EXAMPLE_COUNTY_FIPS)


def get_git_root(path):
    """Return Top Level Git Repository directory given path"""
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return git_root


def zip_shp_to_gdf(zip_file_name):
    """
    Returns a GeoDataFrame from a URL for a zipped Shapefile
    
    https://github.com/agaidus/census_data_extraction/blob/master/census_mapper.py
    """
    zipfile = ZipFile(BytesIO(urlopen(zip_file_name).read()))
    filenames = [y for y in sorted(zipfile.namelist()) for ending in ['dbf', 'prj', 'shp', 'shx']\
                 if y.endswith(ending)] 
    dbf, prj, shp, shx = [BytesIO(zipfile.read(filename)) for filename in filenames]
    r = shapefile.Reader(shp=shp, shx=shx, dbf=dbf)
    
    attributes, geometry = [], []
    field_names = [field[0] for field in r.fields[1:]]  
    for row in r.shapeRecords():  
        geometry.append(shape(row.shape.__geo_interface__))  
        attributes.append(dict(zip(field_names, row.record)))  
        
    gdf = gpd.GeoDataFrame(data = attributes, geometry = geometry)
    return gdf


def conduct_stat_tests(x, y):
    """
    Conduct Statistical Tests
    
    Rank Sum (aka Mann Whitney)
    T Test
    KS Test
    
    Note, all tests here have been benchmarked against R. See for example
    https://asconfluence.stanford.edu/confluence/display/REGLAB/Perform+T-Test+(e.g.+on+Mean)+in+Python+and+R
    """
    wt_stat = stats.mannwhitneyu(x, y, alternative='two-sided', use_continuity=True).statistic
    wt_p = np.round(stats.mannwhitneyu(x, y, alternative='two-sided', use_continuity=True).pvalue, 5)
    
    ks_stat = stats.ks_2samp(x, y).statistic
    ks_p = np.round(stats.ks_2samp(x, y).pvalue, 5)
    
    tt_stat = stats.ttest_ind(x, y, equal_var=False).statistic
    tt_p = np.round(stats.ttest_ind(x, y, equal_var=False).pvalue, 5)
    
    return wt_stat, wt_p, ks_stat, ks_p, tt_stat, tt_p


def main():
    git_root_dir = get_git_root(os.path.dirname(__file__))
    with ENGINE.begin() as conn:
        acs_combined_bg = pd.read_sql(ACS_QUERY, conn)
        ejscreen_df = pd.read_sql(EJ_QUERY, conn)
        facilities_df = pd.read_sql(FACILITIES_QUERY, conn)

    acs_combined_bg = acs_combined_bg.merge(
        right=ejscreen_df[['census_block_group_id', 'p_pwdis']],
        how='left',
        on='census_block_group_id'
    )
    all_ca_cbg_geo = zip_shp_to_gdf(ZIPPED_SHP_URL)
    example_county_cbg_geo = all_ca_cbg_geo[all_ca_cbg_geo.COUNTYFP == EXAMPLE_COUNTY_FIPS]
    example_county_cbg_geo.crs = 4326

    acs_combined_bg = acs_combined_bg.merge(
        right=example_county_cbg_geo[['GEOID', 'geometry']],
        how='left',
        left_on='census_block_group_id',
        right_on='GEOID'
    )

    facilities_df = facilities_df.merge(
        right=acs_combined_bg[['census_block_group_id', 'share_ba', 'median_hh_income', 'prop_nonwhite', 'p_pwdis']],
        how='left',
        on='census_block_group_id'
    )

    # Generating Figure
    acs_combined_bg_gdf = gpd.GeoDataFrame(acs_combined_bg, geometry='geometry')
    fig = plt.figure(figsize=(20, 15))
    gs = gridspec.GridSpec(6, 2)
    ax_lst = [plt.subplot(gs[0:6, 0]), plt.subplot(gs[1:5, 1])]

    acs_combined_bg_gdf.loc[acs_combined_bg_gdf['median_hh_income'] > 0].plot(
        column='median_hh_income',
        ax=ax_lst[0],
        scheme='QUANTILES',
        cmap='Purples',
        legend=True,
        legend_kwds={
            'labels': [
                '[41K, 86K]',
                '(86K, 108K]',
                '(108K, 133K]',
                '(133K, 173K]',
                '(173K, 250K]'
            ],
            'fontsize': 18,
        }
    )

    ax_lst[0].set_title('San Mateo County \n {}'.format('Median Household Income'), fontsize=30)

    ax_lst[0].scatter(x=facilities_df['geocode_longitude'], y=facilities_df['geocode_latitude'], color='orange')
    ax_lst[0].get_xaxis().set_visible(False)
    ax_lst[0].set_ylim(bottom=37.335, top=37.71)
    ax_lst[0].get_yaxis().set_visible(False)

    ####################
    # Now plot Histogram
    ####################
    census_series = acs_combined_bg_gdf.loc[acs_combined_bg_gdf['median_hh_income'] > 0]['median_hh_income']
    npdes_facilities_series = facilities_df.loc[facilities_df['median_hh_income'] > 0]['median_hh_income']
    wt_stat, wt_p, ks_stat, ks_p, tt_stat, tt_p = conduct_stat_tests(census_series, npdes_facilities_series)

    sns.distplot(
        a=census_series,
        ax=ax_lst[1],
        hist=True,
        kde=False,
        color='blue',
        label='County',
        bins=15,
    )

    sns.distplot(
        a=npdes_facilities_series,
        ax=ax_lst[1],
        hist=True,
        kde=False,
        color='red',
        label='NPDES Facilities',
        bins=15,
    )

    ax_lst[1].set_title("""
        San Mateo County
        {}
        """.format('Median Household Income'), fontsize=30)

    ax_lst[1].annotate('Rank Sum: pval = {}   T-Test: pval = {}   KS Test: pval = {}'.format(wt_p, tt_p, ks_p),
                xy=(0.01, 1.04),
                xycoords='axes fraction',
                horizontalalignment='left',
                verticalalignment='top',
                fontsize=16, 
                color='black')

    ax_lst[1].legend(prop={'size': 22})
    ax_lst[1].xaxis.label.set_visible(False)
    ax_lst[1].tick_params(axis='x', labelsize=16)
    ax_lst[1].get_yaxis().set_visible(False)

    fig.tight_layout()
    fig.show()
    fig.savefig(os.path.join(git_root_dir, 'output', 'figures', 'figure1_san_mateo_median_hh_income.pdf'), bbox_inches='tight')


if __name__ == "__main__":
    main()

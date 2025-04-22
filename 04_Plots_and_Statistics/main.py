import os
from google.cloud import bigquery
import seaborn as sb
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import matplotlib.style as style


def exec_select_query(query):
    # Get the data from BigQuery
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google.json")
    client = bigquery.Client()
    result_df = client.query(query).to_dataframe()
    return result_df


def plot_satellite_country_channel_distribution(result_df):
    # Make the plot pretty?
    sb.set_theme(style="whitegrid", rc={"axes.labelweight": 'bold'})
    sb.set_context("paper", font_scale=1.5)
    style.use('tableau-colorblind10')
    # style.use('seaborn-colorblind')
    sb.set_style("ticks")

    # Manipulate data to remove languages with low occurrences
    result_df = result_df.replace("ASTRA 19.2E", "Astra").replace("EUTELSAT HOTBIRD 13.0E", "Hotbird").replace("EUTELSAT 16A 16.0E", "Eutelsat")
    occurrence_threshold = 5
    lows = result_df.groupby('satelliteName').apply(lambda df: df[df['occurrence'] <= occurrence_threshold]['occurrence'].sum())

    result_df = result_df[result_df.occurrence > occurrence_threshold]
    for index, value in lows.items():
        df_other = pd.DataFrame({
            "country": "Other",
            "satelliteName": index,
            "occurrence": value
        }, index=[len(result_df.index)])
        result_df = pd.concat([result_df, df_other])

    df_pivot = pd.pivot_table(result_df, index='satelliteName', columns='country', values='occurrence', aggfunc='sum')
    df_pivot.plot.bar(stacked=True)

    plt.legend(title='Country', loc="upper left", bbox_to_anchor=(1, 1))
    plt.xlabel("Satellite")
    plt.xticks(rotation=45)
    plt.ylabel('Number of channels')
    plt.tight_layout()

    # Show / store plot.
    # plt.show(block=True)
    plt.savefig(os.path.join(os.getcwd(), 'plots', 'p1_channel_country_per_satellite.pdf'), dpi=600,
                transparent=False,
                bbox_inches='tight', format="pdf")


def main():
    result_df = exec_select_query("SELECT satelliteName, country, COUNT(*) as occurrence "
                                  "FROM `hbbtv-research.hbbtv.channel_details` "
                                  "WHERE analyzed GROUP BY satelliteName, country;")
    plot_satellite_country_channel_distribution(result_df)


if __name__ == '__main__':
    main()

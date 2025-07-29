import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
import time

class NCAADataCollector:
    """
    A class to collect NCAA basketball data, including team statistics and tournament results.
    """
    def __init__(self):
        self.base_url = "https://www.ncaa.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def _get_soup(self, url):
        """
        Helper method to get BeautifulSoup object from a URL.
        Includes basic error handling and rate limiting.
        """
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None
        finally:
            time.sleep(1) # Be polite and avoid overwhelming the server

    def get_team_stats_data(self, url):
        """
        Pulls team statistics data into a dataframe from a given URL.
        """
        soup = self._get_soup(url)
        if soup:
            table = soup.find('table')
            if table:
                try:
                    df = pd.read_html(StringIO(table.prettify()))[0]
                    return df
                except Exception as e:
                    print(f"Error parsing table from {url}: {e}")
                    return None
            else:
                print(f"No table found on {url}")
                return None
        return None

    def _get_subsequent_links(self, soup, base_url_prefix="https://www.ncaa.com/stats/basketball-men/d1/"):
        """
        Pulls all subsequent page links for team stats.
        """
        links = []
        # Find all 'a' tags where href contains 'stats' and starts with the base_url_prefix
        all_a_tags = soup.find_all("a", attrs={"href": re.compile(f"^{base_url_prefix}.*stats")})
        
        for item in all_a_tags:
            href = item.get("href")
            if href and href.startswith(base_url_prefix):
                links.append(href)
        
        # Remove duplicates and ensure unique order
        unique_links = []
        seen = set()
        for link in links:
            if link not in seen:
                unique_links.append(link)
                seen.add(link)
        
        # The original script had a specific issue with duplicate page 2s
        # This general approach of unique links should handle it, but if a specific
        # pattern of duplication persists, more targeted logic might be needed.
        return unique_links

    def get_full_stat_dataframe(self, stat_ids):
        """
        Returns a full pandas dataframe of every statistic on the NCAA website
        for the given stat_ids.
        """
        all_dfs = []
        for stat_id in stat_ids:
            initial_url = f'{self.base_url}/stats/basketball-men/d1/current/team/{stat_id}'
            print(f"Collecting data for stat ID: {stat_id} from {initial_url}")
            
            initial_soup = self._get_soup(initial_url)
            if not initial_soup:
                continue

            # Get all subsequent pages for this stat category
            subsequent_links = self._get_subsequent_links(initial_soup, base_url_prefix=f'{self.base_url}/stats/basketball-men/d1/current/team/{stat_id}/')
            
            # Include the initial page if it's not already in subsequent_links
            if initial_url not in subsequent_links:
                subsequent_links.insert(0, initial_url)

            df_list_for_stat = []
            for link in subsequent_links:
                df = self.get_team_stats_data(link)
                if df is not None:
                    df_list_for_stat.append(df)
            
            if df_list_for_stat:
                combined_df_for_stat = pd.concat(df_list_for_stat, ignore_index=True)
                # Ensure 'Team' column is string type for merging
                combined_df_for_stat['Team'] = combined_df_for_stat['Team'].astype(str)
                all_dfs.append(combined_df_for_stat)
            else:
                print(f"No data collected for stat ID: {stat_id}")

        if not all_dfs:
            print("No dataframes collected for any stat ID.")
            return pd.DataFrame()

        # Initialize master_df with the first collected dataframe
        master_df = all_dfs[0].copy()

        # Merge subsequent dataframes
        for i, df in enumerate(all_dfs[1:]):
            # Identify overlapping columns other than 'Team'
            dup_cols = [col for col in df.columns if col in master_df.columns and col != 'Team']
            df_to_merge = df.drop(columns=dup_cols, errors='ignore')
            master_df = pd.merge(master_df, df_to_merge, on='Team', how='outer')
            
        master_df.fillna(0, inplace=True) # Fill NaNs after all merges
        return master_df

    def get_tournament_bracket_data(self, year):
        """
        Scrapes tournament bracket data (teams, seeds, regions, game outcomes) for a given year.
        This is crucial for training the Bayesian model and running simulations.
        """
        print(f"Collecting tournament bracket data for {year}...")
        bracket_url = f"{self.base_url}/brackets/basketball-men/d1/{year}/full-bracket"
        soup = self._get_soup(bracket_url)
        if not soup:
            return None

        tournament_data = []

        # Find all game matchups. This part is highly dependent on NCAA.com's HTML structure.
        # The following is a generalized approach and might need specific selectors.
        # Look for elements that represent individual games or rounds.
        
        # Example: Try to find game blocks, then extract team names and scores
        # This is a placeholder and will need precise CSS selectors based on inspection.
        game_blocks = soup.find_all(class_=re.compile(r'game-pod|game-details')) # Common class names

        if not game_blocks:
            print(f"No game blocks found for {year} on {bracket_url}. HTML structure might have changed.")
            # Fallback: try to find seedings and teams if game outcomes are hard to parse
            return self._get_tournament_seeds_and_teams(soup, year)

        for block in game_blocks:
            try:
                # Extract team names
                team_names = [t.get_text(strip=True) for t in block.find_all(class_=re.compile(r'team-name|participant-name'))]
                # Extract scores
                scores = [s.get_text(strip=True) for s in block.find_all(class_=re.compile(r'score|team-score'))]
                
                if len(team_names) == 2 and len(scores) == 2:
                    team1, team2 = team_names[0], team_names[1]
                    score1, score2 = int(scores[0]), int(scores[1])
                    winner = team1 if score1 > score2 else team2
                    loser = team2 if score1 > score2 else team1
                    
                    # Attempt to get round information (e.g., 'First Four', 'First Round', 'Second Round', etc.)
                    # This might require navigating up the DOM tree or looking for specific headers
                    round_info_element = block.find_previous(class_=re.compile(r'round-name|bracket-round-title'))
                    round_name = round_info_element.get_text(strip=True) if round_info_element else 'Unknown Round'

                    # Attempt to get region information
                    region_info_element = block.find_previous(class_=re.compile(r'region-name|bracket-region-title'))
                    region_name = region_info_element.get_text(strip=True) if region_info_element else 'Unknown Region'

                    tournament_data.append({
                        'Year': year,
                        'Round': round_name,
                        'Region': region_name,
                        'Team1': team1,
                        'Team2': team2,
                        'Score1': score1,
                        'Score2': score2,
                        'Winner': winner,
                        'Loser': loser
                    })
            except Exception as e:
                # print(f"Could not parse game block: {block.get_text(strip=True)[:100]}... Error: {e}")
                continue # Skip malformed blocks
        
        if not tournament_data:
            print(f"No detailed game data parsed for {year}. Attempting to get seeds and teams only.")
            return self._get_tournament_seeds_and_teams(soup, year)

        return pd.DataFrame(tournament_data)

    def _get_tournament_seeds_and_teams(self, soup, year):
        """
        Fallback to scrape just team names and seeds if detailed game data is hard to parse.
        """
        print(f"Attempting to scrape seeds and teams for {year}...")
        seed_data = []
        # This also depends on HTML structure. Look for elements containing seed and team name.
        # Example: <span class="seed">1</span> <span class="team-name">Gonzaga</span>
        team_seed_elements = soup.find_all(class_=re.compile(r'team-item|bracket-team'))

        for element in team_seed_elements:
            try:
                seed = element.find(class_=re.compile(r'seed')).get_text(strip=True) if element.find(class_=re.compile(r'seed')) else None
                team_name = element.find(class_=re.compile(r'team-name|participant-name')).get_text(strip=True) if element.find(class_=re.compile(r'team-name|participant-name')) else None
                if seed and team_name:
                    seed_data.append({'Year': year, 'Team': team_name, 'Seed': seed})
            except Exception as e:
                # print(f"Could not parse team/seed element: {element.get_text(strip=True)[:50]}... Error: {e}")
                continue
        
        if not seed_data:
            print(f"No seed or team data found for {year}.")

        return pd.DataFrame(seed_data)


# Example Usage:
if __name__ == "__main__":
    collector = NCAADataCollector()

    # Example 1: Get full team statistics (similar to your original script)
    # These are the stat IDs from your original data_collection.py
    all_stats_ids = [
        '474','216','1284','214', '1288', '1285', '148', '149' , '286',
        '638', '150', '633', '151', '859', '857', '932', '146', '147',
        '145', '215', '625', '152', '518', '153', '519', '931', '217', '168'
    ]
    # master_team_stats_df = collector.get_full_stat_dataframe(all_stats_ids)
    # if not master_team_stats_df.empty:
    #     print("\nMaster Team Stats DataFrame Head:")
    #     print(master_team_stats_df.head())
    #     master_team_stats_df.to_csv('ncaa_team_stats.csv', index=False)
    #     print("Saved NCAA team stats to ncaa_team_stats.csv")

    # Example 2: Get tournament bracket data for a specific year
    # This is the new functionality you requested
    tournament_year = 2024 # You can change this to any year with available data
    tournament_results_df = collector.get_tournament_bracket_data(tournament_year)
    if tournament_results_df is not None and not tournament_results_df.empty:
        print(f"\nTournament Results for {tournament_year} DataFrame Head:")
        print(tournament_results_df.head())
        tournament_results_df.to_csv(f'ncaa_tournament_results_{tournament_year}.csv', index=False)
        print(f"Saved {tournament_year} tournament results to ncaa_tournament_results_{tournament_year}.csv")
    else:
        print(f"Failed to retrieve tournament results for {tournament_year}.")

    # You can loop through multiple years to get historical tournament data
    # for year in range(2015, 2025): # Example for 10 years
    #     df = collector.get_tournament_bracket_data(year)
    #     if df is not None and not df.empty:
    #         df.to_csv(f'ncaa_tournament_results_{year}.csv', index=False)
    #         print(f"Saved {year} tournament results.")
    #     else:
    #         print(f"Could not get tournament results for {year}.")




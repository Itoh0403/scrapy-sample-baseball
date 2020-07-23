import pandas as pd
import numpy as np
from tqdm import tqdm


def add_sabr_column(df, column, sr_func):
    col_list = []
    for index in df.index:
        m = df.loc[index]
        col_list.append(sr_func(m))
    df[column] = col_list
    return df


def single(h, double, triple, hr):
    return int(h - double - triple - hr)

def sr_single(sr_bat):
    return single(sr_bat["h"], sr_bat["doub"], sr_bat["triple"], sr_bat["hr"])


def df_single(df_bat):
    for index, row in df_bat.iterrows():
        df_bat.loc[index, "single"] = sr_single(row)
    return df_bat


def woba(bb, hbp, single, double, triple, hr, ab, sf):
    u_bb = 0.692
    u_hbp = 0.73
    u_single = 0.865
    u_double = 1.334
    u_triple = 1.725
    u_hr = 2.065
    return (u_bb*bb + u_hbp*hbp + u_single*single + u_double*double + u_triple*triple + u_hr*hr)/(ab + bb + hbp + sf)


def sr_woba(sr_bat):
    return woba(sr_bat["bb"], sr_bat["hbp"], sr_bat["single"], sr_bat["doub"], sr_bat["triple"], sr_bat["hr"],
                sr_bat["ab"], sr_bat["sf"])


def df_woba(df_bat):
    for index, row in df_bat.iterrows():
        df_bat.loc[index, "wOBA"] = sr_woba(row)
    return df_bat


def ops(obp, slg):
    return (obp + slg)


def sr_ops(sr_bat):
    return ops(sr_bat["obp"], sr_bat["slg"])


def leaguesum(df, column):
    central = ["giants", "baystars", "tigers", "carp", "dragons", "swallows"]
    central_list = []
    pacific_list = []
    for index in df.index:
        m = df.loc[index]
        if not m["isPitcher"]:
            if not pd.isnull(m[column]):
                if m["team"] in central:
                    central_list.append(m[column])
                else:
                    pacific_list.append(m[column])
    central_sum = np.sum(central_list)
    pacific_sum = np.sum(pacific_list)
    return central_sum, pacific_sum


def wraa(woba, woba_ave, ab, bb, hbp, sf):
    return (woba - woba_ave)/1.24 * (ab + bb + hbp + sf)


def sr_wraa(sr_bat, woba_ave):
    return wraa(sr_bat["wOBA"], woba_ave, sr_bat["ab"], sr_bat["bb"], sr_bat["hbp"], sr_bat["sf"])


def wrc(wraa, pa, league_r, league_pa):
    return (wraa + league_r / league_pa * pa)


def sr_wrc(sr_bat, league_r, league_pa):
    return wrc(sr_bat["wRAA"], sr_bat["pa"], league_r, league_pa)


def wrcp(wrc, pa, league_r, league_pa):
    return (wrc / pa) / (league_r / league_pa) * 100


def sr_wrcp(sr_bat, league_r, league_pa):
    return wrcp(sr_bat["wRC"], sr_bat["pa"], league_r, league_pa)


def babip(h, hr, pa, so, bb, hbp):
    return (h - hr)/(pa - so - hr - bb - hbp)


def sr_babip(sr_bat):
    return babip(sr_bat["h"], sr_bat["hr"], sr_bat["pa"], sr_bat["so"], sr_bat["bb"], sr_bat["hbp"])


def add_isPitcher(df_bat, df_pitch):
    p_list = []
    for index in df_bat.index:
        if df_bat.loc[index]["name"] in list(df_pitch.name):
            p_list.append(True)
        else:
            p_list.append(False)
    df_bat.loc[:, "isPitcher"] = p_list
    return df_bat


def league_series_bat(df_bat, league, year):
    league_list = ["Central", "Pacific"]
    sr_league = pd.Series(
        [year, league_list[league], league_list[league], "T", 143 * 6, leaguesum(df_bat, "pa")[league],
         leaguesum(df_bat, "ab")[league],
         leaguesum(df_bat, "r")[league], leaguesum(df_bat, "h")[league], leaguesum(df_bat, "doub")[league],
         leaguesum(df_bat, "triple")[league], leaguesum(df_bat, "hr")[league], leaguesum(df_bat, "tb")[league],
         leaguesum(df_bat, "rbi")[league], leaguesum(df_bat, "so")[league], leaguesum(df_bat, "bb")[league],
         leaguesum(df_bat, "ibb")[league],
         leaguesum(df_bat, "hbp")[league], leaguesum(df_bat, "sh")[league], leaguesum(df_bat, "sf")[league],
         leaguesum(df_bat, "sb")[league],
         leaguesum(df_bat, "cs")[league], leaguesum(df_bat, "dp")[league]], index=df_bat.columns[:23])
    sr_league["single"] = int(leaguesum(df_bat, "single")[league])
    sr_league["ba"] = np.round(sr_league["h"]/sr_league["ab"], 4)
    sr_league["slg"] = np.round(sr_league["tb"] / sr_league["ab"], 4)
    sr_league["obp"] = np.round((sr_league["h"] + sr_league["bb"] + sr_league["hbp"]) / sr_league["pa"], 4)
    sr_league["ops"] = np.round(sr_league["obp"] + sr_league["slg"], 4)
    sr_league["wOBA"] = np.round(sr_woba(sr_league), 4)
    return sr_league


def calculate_sabr(df_bat, df_pitcher, year):
    df_bat = df_bat[df_bat["year"] == year]
    df_pitch = df_pitcher[df_pitcher["year"] == year]
    df_bat = df_single(df_bat)
    df_bat = add_isPitcher(df_bat, df_pitch)
    df_bat = df_bat.query("ab > 0")
    df_bat = df_woba(df_bat)
    sr_central = league_series_bat(df_bat, 0, year)
    sr_pacific = league_series_bat(df_bat, 1, year)
    central = ["giants", "baystars", "tigers", "carp", "dragons", "swallows"]
    for index in tqdm(df_bat.index):
        if df_bat.loc[index, "team"] in central:
            df_bat.loc[index, "wRAA"] = np.round(sr_wraa(df_bat.loc[index], sr_central["wOBA"]), 2)
            df_bat.loc[index, "wRC"] = np.round(sr_wrc(df_bat.loc[index], sr_central["r"], sr_central["pa"]), 2)
            df_bat.loc[index, "wRCP"] = np.round(sr_wrcp(df_bat.loc[index], sr_central["r"], sr_central["pa"]), 2)
            df_bat.loc[index, "babip"] = np.round(sr_babip(df_bat.loc[index]), 4)
        else:
            df_bat.loc[index, "wRAA"] = np.round(sr_wraa(df_bat.loc[index], sr_pacific["wOBA"]), 2)
            df_bat.loc[index, "wRC"] = np.round(sr_wrc(df_bat.loc[index], sr_pacific["r"], sr_pacific["pa"]), 2)
            df_bat.loc[index, "wRCP"] = np.round(sr_wrcp(df_bat.loc[index], sr_pacific["r"], sr_pacific["pa"]), 2)
            df_bat.loc[index, "babip"] = np.round(sr_babip(df_bat.loc[index]), 4)
    return df_bat, sr_central, sr_pacific

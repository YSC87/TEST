import argparse
import json
import logging
import os
import sys
from types import SimpleNamespace as Namespace
from collections import defaultdict
import pandas as pd
import utils.etl_util as etlu
import utils.misc_util as miscu

RETURN_SUCCESS = 0
RETURN_FAILURE = 1
APP = 'EtlData utility'


def main(argv):
    try:
        # Parse command line arguments.
        args, process_name, process_type, process_config = _interpret_args(argv)

        # Initialize standard logging \ destination file handlers.
        std_filename = "/Users/ysc/Desktop/6861/fall2022py/etldata.log"
        logging.basicConfig(filename=std_filename,
                            filemode='a',
                            format='%(asctime)s - %(message)s',
                            level=logging.INFO)
        logging.info('-------------------------------------------------')
        logging.info(f'Entering {APP}')

        # Preparation step.
        mapping_args = miscu.convert_namespace_to_dict(args)
        mapping_conf = miscu.convert_namespace_to_dict(process_config)

        # Workflow steps.
        if process_type == 'extraction':
            run_extraction(mapping_args, mapping_conf)
        elif process_type == 'transformation':
            run_transformation(mapping_args, mapping_conf)
        else:
            logging.warning(f'Incorrect feature type: [{process_type}]')

        logging.info(f'Leaving {APP}')
        return RETURN_SUCCESS
    except FileNotFoundError as nf_error:
        logging.error(f'Leaving {APP} incomplete with errors')
        return f'ERROR: {str(nf_error)}'
    except KeyError as key_error:
        logging.error(f'Leaving {APP} incomplete with errors')
        return f'ERROR: {key_error.args[0]}'
    except Exception as gen_exc:
        logging.error(f'Leaving {APP} incomplete with errors')
        raise gen_exc


def _interpret_args(argv):
    """
    Read, parse, and interpret given command line arguments.
    Also, define default value.
    :param argv: Given argument parameters.
    :return: Full mapping of arguments, including all default values.
    """
    arg_parser = argparse.ArgumentParser(APP)
    arg_parser.add_argument('-log', dest='log_path', help='Fully qualified logging file')
    arg_parser.add_argument('-process', dest='process', help='Process type', required=True)

    # Extract and interpret rest of the arguments, using static config file, based on given specific feature.
    process_arg = argv[argv.index('-process') + 1]
    process_args = process_arg.rsplit('_', 1)
    process_name = process_args[0]
    process_type = process_args[1]
    current_path = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(current_path, f'../config/{process_name}.json')) as file_config:
        mapping_config = json.load(file_config, object_hook=lambda d: Namespace(**d))
        if process_type == 'extraction':
            process_config = vars(mapping_config.extraction)
        elif process_type == 'transformation':
            process_config = vars(mapping_config.transformation)

        feature_args = vars(mapping_config.feature_args)
        # Add necessary arguments to <arg_parser> instance, using static JSON-based configuration.
        if feature_args:
            for key, value in feature_args.items():
                if isinstance(value, Namespace):
                    value = vars(value)
                arg_parser.add_argument(key, dest=value['dest'], help=value['help'],
                                        required=(value['required'] == "True"))
    return arg_parser.parse_args(argv), process_name, process_type, process_config


def run_extraction(args, config):
    # --------------------------------
    # Input section
    # --------------------------------

    # Prepare additional input parameters and update appropriate configuration section.
    # Inject 'path' and 'description' into <input> config section.
    input_update_with = {'path': miscu.eval_elem_mapping(args, 'input_path'), 'description': config['description']}
    input_config = miscu.eval_elem_mapping(config, 'input')
    input_read_config = miscu.eval_update_mapping(input_config, "read", input_update_with)

    # Run read ETL feature.
    df_target = etlu.read_feature(input_read_config)

    # Engage plugin from <input> config section, if available.
    input_plugin = miscu.eval_elem_mapping(input_config, "plugin")
    if input_plugin:
        df_target = input_plugin(df_target)

    # --------------------------------
    # Mapping section
    # --------------------------------

    # Prepare additional mapping parameters and update appropriate configuration section.
    # Inject 'path' and 'description' into <mapping> config section.
    mapping_update_with = {'path': miscu.eval_elem_mapping(args, 'mapping_path'), 'description': config['description']}
    mapping_config = miscu.eval_elem_mapping(config, 'mapping')
    _ = miscu.eval_update_mapping(mapping_config, 'read', mapping_update_with)

    # Run mapping ETL feature.
    df_target = etlu.mapping_feature(df_target, mapping_config)

    # --------------------------------
    # Additional processing
    # --------------------------------
    output_config = miscu.eval_elem_mapping(config, 'output')

    # Rename columns
    cols_rename_mapping = miscu.eval_elem_mapping(output_config, 'col_rename')
    df_target.rename(columns=cols_rename_mapping, inplace=True)

    # Assign statics
    assign_statics = miscu.eval_elem_mapping(output_config, 'assign_static')
    for col, value in assign_statics.items():
        df_target[col] = value

    # --------------------------------
    # Output section
    # --------------------------------

    # Prepare additional output parameters and update appropriate configuration section.
    # Inject 'path' and 'description' into <output> config section.
    output_update_with = {'path': miscu.eval_elem_mapping(args, 'output_path'), 'description': config['description']}
    output_write_config = miscu.eval_update_mapping(output_config, 'write', output_update_with)

    # If col_output is set in the config, output will only contain the specified columns
    output_columns = miscu.eval_elem_mapping(output_config, 'col_output', None)
    if output_columns is not None:
        df_target = df_target[output_columns]

    etlu.write_feature(df_target, output_write_config)

    # Engage plugin from <output> config section, if available.
    output_plugin = miscu.eval_elem_mapping(output_config, "plugin")
    if output_plugin:
        df_target = output_plugin(df_target)

    return df_target


def run_transformation(args, config):
    # --------------------------------
    # Input section
    # --------------------------------

    # input config
    input_update_with = {'path': miscu.eval_elem_mapping(args, 'input_path'), 'description': config['description']}
    input_config = miscu.eval_elem_mapping(config, 'input')
    input_read_config = miscu.eval_update_mapping(input_config, "read", input_update_with)

    # Run read ETL feature.
    df = etlu.read_feature(input_read_config)

    # transformation config
    transformation_config = miscu.eval_elem_mapping(config, 'transformation')

    # a dictionary recording all stats
    memo = {}

    # group by country
    if transformation_config['group_by'] == "country":

        # Amount of Attendance
        year_country_pairs = set()
        for _, row in df.iterrows():
            year_country_pairs.add((row['year'], row['team']))
        participation_count = defaultdict(int)
        for _, country in year_country_pairs:
            participation_count[country] += 1
        memo["Amt of Attendance"] = sorted(participation_count.items(), key=lambda pair: -pair[1])

        # Amount of Goals
        goal_count = df["team"].value_counts()
        memo["Amt of Goals"] = [pair for pair in zip(goal_count.index, goal_count.values)]

        # Amount of Goals in a Single Match
        year_country_goal = df.groupby("match_id")['team'].value_counts()
        memo["Amt of Goals in a Single Match"] = sorted([(pair[0][1], pair[0][0], pair[1]) for pair in
                                                         zip(year_country_goal.index, year_country_goal.values)],
                                                        key=lambda pair: -pair[2])

        # Amount of Players who Scored
        player_count = df.groupby("team")['player_name'].nunique()
        memo["Amt of Players who Scored"] = sorted([pair for pair in zip(player_count.index, player_count.values)],
                                                   key=lambda pair: -pair[1])

        # Amount of Own Goals
        country_own_goal_pairs = df.groupby("team")['own_goal'].sum()
        memo["Amt of Own Goals"] = sorted(
            [pair for pair in zip(country_own_goal_pairs.index, country_own_goal_pairs.values)],
            key=lambda pair: -pair[1])

        # Average Age of the Players when Scoring
        df['age'] = [int(timedelta.days / 365.25) for timedelta in (df['match_date'] - df['birth_date'])]
        country_age_pairs = df.groupby("team")['age'].mean()
        memo["Avg Age of Players when Scoring"] = sorted(
            [(cntry, round(age, 1)) for cntry, age in zip(country_age_pairs.index, country_age_pairs.values)],
            key=lambda pair: -pair[1])

    # group by player
    if transformation_config['group_by'] == "player":

        # Amount of Attendance
        participation_count = []
        for num in (6, 5, 4, 3):
            for player in df.loc[df['count_tournaments'] == num, 'player_name'].unique():
                participation_count.append((player, num))
        memo["Amt of Attendance"] = participation_count

        # Amount of Goals in Life
        goal_count = df["player_name"].value_counts()
        memo["Amt of Goals in Life"] = [pair for pair in zip(goal_count.index, goal_count.values)]

        # Amount of Goals in a Single Year
        year_player_goal = df.groupby("year")['player_name'].value_counts()
        memo["Amt of Goals in a Single Year"] = sorted([(pair[0][1], pair[0][0], pair[1]) for pair in
                                                        zip(year_player_goal.index, year_player_goal.values)],
                                                       key=lambda pair: -pair[2])

        # Amount of Goals in a Single Match
        year_player_goal = df.groupby("match_id")['player_name'].value_counts()
        memo["Amt of Goals in a Single Match"] = sorted([(pair[0][1], pair[0][0], pair[1]) for pair in
                                                         zip(year_player_goal.index, year_player_goal.values)],
                                                        key=lambda pair: -pair[2])

        # Amount of Hat Tricks
        player_goal_table = defaultdict(int)
        for match, player in zip(df['match_id'], df['player_name']):
            player_goal_table[(match, player)] += 1
        amt_of_hat_tricks = defaultdict(int)
        for pair, goal in player_goal_table.items():
            if goal >= 3:
                amt_of_hat_tricks[pair[1]] += 1
        memo["Amt of Hat Tricks"] = sorted(amt_of_hat_tricks.items(), key=lambda pair: -pair[1])

        # Age of Players when Scoring
        df['age'] = [int(timedelta.days / 365.25) for timedelta in (df['match_date'] - df['birth_date'])]
        player_age_pairs = [(player, age) for player, age in zip(df['player_name'], df['age'])]
        memo["Age of Players when Scoring"] = sorted(player_age_pairs, key=lambda pair: -pair[1])

    summary = pd.DataFrame(dict([(metric, pd.Series(stat)) for metric, stat in memo.items()]))
    summary.index = range(1, len(summary) + 1)
    summary.index.name = "Rank"

    # show only the top n rows
    if transformation_config['top_n'] > 0:
        summary = summary.iloc[:transformation_config['top_n'], ]

    # output config
    output_config = miscu.eval_elem_mapping(config, 'output')
    output_update_with = {'path': miscu.eval_elem_mapping(args, 'output_path'), 'description': config['description']}
    output_write_config = miscu.eval_update_mapping(output_config, 'write', output_update_with)
    etlu.write_feature(summary, output_write_config)


if __name__ == '__main__':
    # Call main process.
    sys.exit(main(sys.argv[1:]))

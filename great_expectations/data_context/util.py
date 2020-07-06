import copy
import inspect
import logging
import os
import re
from collections import OrderedDict
from typing import List

from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
)
from great_expectations.exceptions import MissingConfigVariableError
from great_expectations.util import load_class, verify_dynamic_loading_support

logger = logging.getLogger(__name__)


# TODO: Rename config to constructor_kwargs and config_defaults -> constructor_kwarg_default
# TODO: Improve error messages in this method. Since so much of our workflow is config-driven, this will be a *super* important part of DX.
def instantiate_class_from_config(config, runtime_environment, config_defaults=None):
    """Build a GE class from configuration dictionaries."""

    if config_defaults is None:
        config_defaults = {}

    config = copy.deepcopy(config)

    module_name = config.pop("module_name", None)
    if module_name is None:
        try:
            module_name = config_defaults.pop("module_name")
        except KeyError:
            raise KeyError(
                "Neither config : {} nor config_defaults : {} contains a module_name key.".format(
                    config, config_defaults,
                )
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("module_name", None)

    verify_dynamic_loading_support(module_name=module_name)

    class_name = config.pop("class_name", None)
    if class_name is None:
        logger.warning(
            "Instantiating class from config without an explicit class_name is dangerous. Consider adding "
            "an explicit class_name for %s" % config.get("name")
        )
        try:
            class_name = config_defaults.pop("class_name")
        except KeyError:
            raise KeyError(
                "Neither config : {} nor config_defaults : {} contains a class_name key.".format(
                    config, config_defaults,
                )
            )
    else:
        # Pop the value without using it, to avoid sending an unwanted value to the config_class
        config_defaults.pop("class_name", None)

    class_ = load_class(class_name=class_name, module_name=module_name)

    config_with_defaults = copy.deepcopy(config_defaults)
    config_with_defaults.update(config)
    if runtime_environment is not None:
        # If there are additional kwargs available in the runtime_environment requested by a
        # class to be instantiated, provide them
        argspec = inspect.getfullargspec(class_.__init__)[0][1:]

        missing_args = set(argspec) - set(config_with_defaults.keys())
        config_with_defaults.update(
            {
                missing_arg: runtime_environment[missing_arg]
                for missing_arg in missing_args
                if missing_arg in runtime_environment
            }
        )
        # Add the entire runtime_environment as well if it's requested
        if "runtime_environment" in missing_args:
            config_with_defaults.update({"runtime_environment": runtime_environment})

    try:
        class_instance = class_(**config_with_defaults)
    except TypeError as e:
        raise TypeError(
            "Couldn't instantiate class : {} with config : \n\t{}\n \n".format(
                class_name, format_dict_for_error_message(config_with_defaults)
            )
            + str(e)
        )

    return class_instance


def format_dict_for_error_message(dict_):
    # TODO : Tidy this up a bit. Indentation isn't fully consistent.

    return "\n\t".join("\t\t".join((str(key), str(dict_[key]))) for key in dict_)


def find_substitution_candidates(template_str: str) -> List[dict]:
    """
    This method takes a string, looks for patterns like:
    ${SOME_VARIABLE} or $SOME_VARIABLE or ${some_variable} or $some_variable
    or ${sOmE_vAr1234iAblE} or $sOmE_vAr1234iAblE.
    But not ${23rd_VARIABLE} or $23rd_VARIABLE.
    There just has to be a leading $ and the first character cannot be 0-9.
    The key to the substitution string must match the case.
    It returns a dictionary describing where these patterns are within the string.
    It is case insensitive and will return multiple patterns if found.

    :param template_str: str that may contain substitution candidates
    """
    matches = re.finditer(r"\$\{(.*?)\}|\$([_a-zA-Z][_a-zA-Z0-9]*)", template_str)

    substitution_candidates = []

    for match in matches:

        # TODO: This can be done more elegantly
        # Remove all $ and curly braces
        dirty_match = match.group()
        clean_match = dirty_match.replace("$", "")
        clean_match = clean_match.replace("{", "")
        clean_match = clean_match.replace("}", "")

        substitution_candidates.append(
            {
                "match": dirty_match,
                "clean_match": clean_match,
                "start": match.start(),
                "end": match.end(),
            }
        )

    return substitution_candidates


def substitute_config_variable(template_str, config_variables_dict):
    """
    This method takes a string, and if it contains a pattern ${SOME_VARIABLE} or $SOME_VARIABLE,
    returns a string where the pattern is replaced with the value of SOME_VARIABLE,
    otherwise returns the string unchanged. SOME_VARIABLE is case insensitive.

    If the environment variable SOME_VARIABLE is set, the method uses its value for substitution.
    If it is not set, the value of SOME_VARIABLE is looked up in the config variables store (file).
    If it is not found there, the input string is returned as is.

    :param template_str: a string that might or might not be of the form ${SOME_VARIABLE}
            or $SOME_VARIABLE
    :param config_variables_dict: a dictionary of config variables. It is loaded from the
            config variables store (by default, "uncommitted/config_variables.yml file)
    :return:
    """
    if template_str is None:
        return template_str

    substitution_candidates = find_substitution_candidates(template_str)

    # TODO: Allow substitution of dicts if there is only one substution_candidate
    # and there is no other data
    if len(substitution_candidates) == 1:
        substitution_candidate = substitution_candidates.pop(0)

        # Find the data to substitute.
        # Check if environment variable exists, if so substitute it
        env_variable = os.getenv(substitution_candidate["clean_match"])
        if env_variable:
            template_str = (
                template_str[: substitution_candidate["start"]]
                + env_variable
                + template_str[substitution_candidate["end"] :]
            )

        # If not, check for match in config_variables_dict, if so substitute
        elif substitution_candidate["clean_match"] in config_variables_dict:
            # If the data to substitute is not a string
            # Check to make sure the substitution string is
            # the only text in the substitution candidate

            if isinstance(
                config_variables_dict[substitution_candidate["clean_match"]], str
            ):
                template_str = (
                    template_str[: substitution_candidate["start"]]
                    + config_variables_dict[substitution_candidate["clean_match"]]
                    + template_str[substitution_candidate["end"] :]
                )
            else:
                if substitution_candidate["match"] == template_str:
                    return config_variables_dict[substitution_candidate["clean_match"]]
                else:
                    raise Exception(
                        "\n\nNon-string substitutions are only allowed for single replacement substitutions "
                        "with no leading or trailing characters."
                        "For example `HOST: ${HOST}` is a single replacement substitution with no leading or "
                        "trailing characters. "
                        "`DB_URL: postgresql://${HOST}/${DB_NAME}` is a multi-replacement substitution with "
                        "leading characters."
                    )

            # # re-calculate substitution candidates in case there are some in
            # # the newly inserted data from config_variables_dict
            # substitution_candidates = find_substitution_candidates(template_str)

        # If value to substitue is not found in either place, raise error
        else:
            raise MissingConfigVariableError(
                f"""\n\nUnable to find a match for config substitution variable: `{substitution_candidate['clean_match']}`.
    Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
    See https://great-expectations.readthedocs.io/en/latest/reference/data_context_reference.html#managing-environment-and-secrets""",
                missing_config_variable=substitution_candidate["clean_match"],
            )

    # Use a queue in case we add more substitution candidates from config_variables_dict
    # Let's not worry much about performance as this should be a small amount of data
    while len(substitution_candidates) > 0:

        substitution_candidate = substitution_candidates.pop(0)

        # Check if environment variable exists, if so substitute it
        env_variable = os.getenv(substitution_candidate["clean_match"])
        if env_variable:
            template_str = (
                template_str[: substitution_candidate["start"]]
                + env_variable
                + template_str[substitution_candidate["end"] :]
            )

        # If not, check for match in config_variables_dict, if so substitute
        elif substitution_candidate["clean_match"] in config_variables_dict:
            template_str = (
                template_str[: substitution_candidate["start"]]
                + config_variables_dict[substitution_candidate["clean_match"]]
                + template_str[substitution_candidate["end"] :]
            )
            # re-calculate substitution candidates in case there are some in
            # the newly inserted data from config_variables_dict
            substitution_candidates = find_substitution_candidates(template_str)

        # If value to substitue is not found in either place, raise error
        else:
            raise MissingConfigVariableError(
                f"""\n\nUnable to find a match for config substitution variable: `{substitution_candidate['clean_match']}`.
    Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
    See https://great-expectations.readthedocs.io/en/latest/reference/data_context_reference.html#managing-environment-and-secrets""",
                missing_config_variable=substitution_candidate["clean_match"],
            )

    # TODO: Determine if any of this is needed, esp the if statement in this part:
    #         if config_variable_value is not None:
    #             if match.start() == 0 and match.end() == len(template_str):
    #                 return config_variable_value
    #             else:
    #                 return (
    #                     template_str[: match.start()]
    #                     + config_variable_value
    #                     + template_str[match.end() :]
    #                 )

    #     # if len(substitution_candidates) > 0:

    #         # For loop to iterate through tuple of matches
    #         outer_matches = match.groups()
    #         # for idx, match in enumerate(outer_matches):
    #         #     start = match.start(idx + 1)
    #         #     end = match.end(idx + 1)

    #         for outer_config_match in outer_matches:

    #             config_variable_value = config_variables_dict.get(outer_config_match)

    #             try:
    #                 inner_match = re.search(r"\$\{(.*?)\}", config_variable_value) or re.search(
    #                     r"\$([_a-zA-Z][_a-zA-Z0-9]*)", config_variable_value
    #                 )
    #             except TypeError:
    #                 inner_match = None

    #             # For

    #         if inner_match:
    #             config_variable_value = os.getenv(inner_match.group(1))

    #         if config_variable_value is not None:
    #             if match.start() == 0 and match.end() == len(template_str):
    #                 return config_variable_value
    #             else:
    #                 return (
    #                     template_str[: match.start()]
    #                     + config_variable_value
    #                     + template_str[match.end() :]
    #                 )

    #         raise MissingConfigVariableError(
    #             f"""\n\nUnable to find a match for config substitution variable: `{match.group(1)}`.
    # Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
    # See https://great-expectations.readthedocs.io/en/latest/reference/data_context_reference.html#managing-environment-and-secrets""",
    #             missing_config_variable=match.group(1),
    #         )

    return template_str


def substitute_all_config_variables(data, replace_variables_dict):
    """
    Substitute all config variables of the form ${SOME_VARIABLE} in a dictionary-like
    config object for their values.

    The method traverses the dictionary recursively.

    :param data:
    :param replace_variables_dict:
    :return: a dictionary with all the variables replaced with their values
    """
    if isinstance(data, DataContextConfig):
        data = DataContextConfigSchema().dump(data)

    if isinstance(data, dict) or isinstance(data, OrderedDict):
        return {
            k: substitute_all_config_variables(v, replace_variables_dict)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [
            substitute_all_config_variables(v, replace_variables_dict) for v in data
        ]
    return substitute_config_variable(data, replace_variables_dict)


def file_relative_path(dunderfile, relative_path):
    """
    This function is useful when one needs to load a file that is
    relative to the position of the current file. (Such as when
    you encode a configuration file path in source file and want
    in runnable in any current working directory)

    It is meant to be used like the following:
    file_relative_path(__file__, 'path/relative/to/file')

    H/T https://github.com/dagster-io/dagster/blob/8a250e9619a49e8bff8e9aa7435df89c2d2ea039/python_modules/dagster/dagster/utils/__init__.py#L34
    """
    return os.path.join(os.path.dirname(dunderfile), relative_path)

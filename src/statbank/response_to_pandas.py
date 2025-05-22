import json
from typing import TYPE_CHECKING
from typing import Any

import numpy as np
import pandas as pd
import requests

if TYPE_CHECKING:
    from numpy.typing import NDArray


class StatbankDataBuildError(Exception):
    """Custom exception for errors that occur during the building of Statbank data."""


def stack_categories(varcodes: dict[str, list[str]]) -> pd.DataFrame:
    """Stacks the categories of the variables (in the correct order) from the response into a DataFrame.

    Args:
        varcodes (dict[str, list[Any]]): A dictionary where the keys are variable names and the values are lists of codes.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the stacked categories.
    """
    lenghts: list[int] = [len(codes) for codes in varcodes.values()]

    nrepeats: int = int(np.prod(lenghts))
    ntiles: int = 1

    data_dict: dict[str, NDArray[Any]] = {}

    var: str
    codes: list[str]

    for var, codes in varcodes.items():
        ncat: int = len(codes)

        # reduce nrepeats before creating the array
        nrepeats = int(nrepeats / ncat)

        # create array
        data_dict[var] = np.repeat(np.tile(codes, reps=ntiles), repeats=nrepeats)

        # reduce ntiles afterwards
        ntiles = int(ncat * ntiles)

    return pd.DataFrame(data_dict)


def response_to_pandas(
    response: requests.models.Response,
    include_id: bool = False,
) -> pd.DataFrame:
    """Converts a response from the SSB Statbank API to a pandas DataFrame.

    Args:
        response (requests.models.Response): The response object from the API.
        include_id (bool): Whether to include the ID columns in the output. Defaults to False.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data from the response.

    Raises:
        StatbankDataBuildError: If the number of categories and values do not match.
    """
    content: dict[str, Any] = json.loads(response.content.decode("UTF-8"))

    dimension: dict[str, Any] = content["dimension"]

    varnames: dict[str, str] = {}
    codes: dict[str, list[str]] = {}
    labels: dict[str, dict[str, str]] = {}

    var: str
    var_info: dict[str, Any]

    for var, var_info in dimension.items():
        varnames[var] = var_info["label"]
        codes[var] = list(var_info["category"]["index"].keys())
        labels[var] = var_info["category"]["label"]

    data: pd.DataFrame = stack_categories(codes)
    value: list[str | float | int] = content["value"]

    if data.shape[0] != len(value):
        message: str = (
            "The number of categories and values do not match! This is likely a bug!"
        )
        raise StatbankDataBuildError(message)

    data["value"] = value

    id_col: str
    id_col2: str
    codes2labels: dict[str, str]

    for id_col, codes2labels in labels.items():
        label_col: str = varnames[id_col]

        # probably a prettier way of doin this
        # linter also complains if we overwrite id_col (id_col = "id_" + id_col)
        if id_col == label_col:  # not sure if this is possible, but just in case
            data = data.rename(columns={id_col: "id_" + id_col})
            id_col2 = "id_" + id_col
        else:
            id_col2 = id_col

        data[label_col] = data[id_col2].map(codes2labels)

    if not include_id:  # early return
        cols: list[str] = [*list(varnames.values()), "value"]
        return data[cols]

    knitted_cols: list[str] = []

    for id_col, label_col in varnames.items():
        # ugly doing this twice
        # linter also complains if we overwrite id_col (id_col = "id_" + id_col)
        id_col2 = "id_" + id_col if id_col == label_col else id_col
        knitted_cols += [id_col2, label_col]

    knitted_cols += ["value"]

    return data[knitted_cols]

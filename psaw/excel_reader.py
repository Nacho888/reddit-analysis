import pandas as pd
import logging
#####
import logging_factory
#####
logger = logging_factory.get_module_logger("excel_reader", logging.DEBUG)
logger_err = logging_factory.get_module_logger("excel_reader_err", logging.ERROR)


def get_queries_and_scales(path: str, start_row: int, scales_column: int, queries_column: int, related_column: int):
    """
    Function to obtain the scales' names and queries

    :param path: str - the path where the excel spreadsheet is located
    :param start_row: int - row number where the first query is located
    :param scales_column: int - column number where the scales are located
    :param queries_column: int - column number where the queries are located
    :param related_column: int - column number where the relationship between queries in different scales is located
    :return: result: dict - a dictionary with scales as keys and list of queries as values
    """

    # Workbook object is created
    spreadsheet = pd.read_excel(path)
    m_row = len(spreadsheet.index)

    result = {}
    current_scale = ""

    for i in range(start_row - 2, m_row):
        scale = spreadsheet.iloc[i, scales_column - 1]
        print(scale)

        # A new scale appears
        if scale not in result and not pd.isna(scale):
            current_scale = scale
            result[scale] = []

        # For merged cells
        if pd.isna(scale):
            scale = current_scale

        query = spreadsheet.iloc[i, queries_column - 1]
        queries = []

        related_code = spreadsheet.iloc[i, related_column - 1]
        codes = []
        related_scales = []

        if related_code is not pd.isna(related_code):
            try:
                related_code = str(related_code)  # Cast to string just in case we have to split it later
                to_check_codes = convert_code(related_code)
                for c in to_check_codes:
                    if code_present(spreadsheet, c, start_row, related_column):
                        codes.append(c)
                    else:
                        logger_err.error("Code '{}' appears only one time".format(c))
                related_scales = get_related_scales(spreadsheet, current_scale, codes, start_row, scales_column,
                                                    related_column)
            except ValueError:
                logger_err.error("Error when casting code in (row: {}, col:{})".format(i, related_column))

        to_add = []
        if not pd.isna(query):
            if "," in query:  # Multiple queries in the same cell
                queries = query.split(",")
            if len(queries) > 0:
                for q in queries:
                    to_add.append(q.strip())
                    to_add.append(codes)
                    to_add.append(related_scales)
                    result[scale].append(to_add)
                    to_add = []
            # Single query
            else:
                to_add.append(query.strip())
                to_add.append(codes)
                to_add.append(related_scales)
                result[scale].append(to_add)

    logger.debug("Scales and queries loaded")
    print(result)

    return result


def get_related_scales(spreadsheet, current_scale: str, codes: list, start_row: int, scales_column: int,
                       related_column: int):
    """
    Function that returns the name of all the scales related to the relationship code given

    :param spreadsheet: the current Excel spreadsheet
    :param current_scale: str - current scale to skip it
    :param codes: list - relationship codes
    :param start_row: int - row number where the first query is located
    :param scales_column: int - column number where the scales are located
    :param related_column: int - column number where the relationship between queries in different scales is located
    :return: result: list - the list with the scale names
    """

    m_row = len(spreadsheet.index)

    result = []

    for code in codes:
        for i in range(m_row - 1, start_row - 2, -1):
            try:
                related_code = str(spreadsheet.iloc[i, related_column - 1])

                if not pd.isna(related_code):  # Empty cell

                    codes = convert_code(related_code)

                    if code in codes:  # Code is present in cell
                        scale = spreadsheet.iloc[i, scales_column - 1]

                        # Row with code, but maybe it doesn't have the scale name
                        # (merged cells - go back until we find the name)
                        j = i
                        while pd.isna(scale):
                            scale = spreadsheet.iloc[j, scales_column - 1]
                            j -= 1

                        if scale not in result and scale != current_scale:

                            result.append(scale)
            except ValueError:
                result = []
                logger_err.error("Error when casting code in (row: {}, col:{})".format(i, related_column))
                break

    return result


def convert_code(code: str):
    """
    Function that given a query relationship code returns a list with the desired code format

    :param code: str - the code to format
    :return: result: list - the list of codes
    """

    result = []
    aux = []

    if "_" in code:
        aux = code.split("_")
    if len(aux) > 0:
        for c in aux:
            result.append(c)
    else:
        result.append(code)

    return result


def code_present(spreadsheet, code: str, start_row: int, related_column: int):
    """
    Function to check if a certain relationship code is present in the spreadsheet

    :param spreadsheet: the current Excel spreadsheet
    :param code: str - the code to check
    :param start_row: int - row number where the first query is located
    :param related_column: int - column number where the relationship between queries in different scales is located
    :return: found: bool - if the code is found or not
    """

    m_row = len(spreadsheet.index)

    found = False  # scale associated has been found
    for i in range(start_row - 2, m_row):
        try:
            related_code = str(spreadsheet.iloc[i, related_column - 1])
        except ValueError:
            found = False
            logger_err.error("Error when casting code in (row: {}, col:{})".format(i, related_column))
            break
        if str(related_code) == code:
            found = True
    return found


queries_and_scales = get_queries_and_scales("./excel/scales.xlsx", 5, 2, 4, 5)

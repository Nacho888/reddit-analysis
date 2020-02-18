import openpyxl
import logging
#####
import logging_factory
#####
logger = logging_factory.get_module_logger("excel_reader", logging.DEBUG)


def get_queries_and_scales(path: str, start_row: int, scales_column: int, queries_column: int):
    """
    Function to obtain the scales' names and queries

    :param path: str - the path where the excel spreadsheet is located
    :param start_row: int - row number where the first query is located
    :param scales_column: int - column number where the scales are located
    :param queries_column: int - column number where the queries are located
    :return: result: dict - a dictionary with scales as keys and list of queries as values

    """

    # Workbook object is created
    wb_obj = openpyxl.load_workbook(path)
    sheet_obj = wb_obj.active
    m_row = sheet_obj.max_row

    result = {}

    for i in range(start_row, m_row + 1):
        scale = sheet_obj.cell(row=i, column=scales_column).value

        # A new scale appears
        if scale not in result and scale is not None:
            current_scale = scale
            result[scale] = []

        # For merged cells
        if scale is None:
            scale = current_scale

        query = sheet_obj.cell(row=i, column=queries_column).value
        queries = []

        if query is not None:
            # Multiple queries in the same cell
            if "," in query:
                queries = query.split(",")
            if len(queries) > 0:
                for q in queries:
                    result[scale].append(q.strip())
            # Single query
            else:
                result[scale].append(query.strip())

    logger.debug("Scales and queries loaded")

    return result

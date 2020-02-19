import openpyxl
import logging
#####
import logging_factory
#####
logger = logging_factory.get_module_logger("excel_reader", logging.DEBUG)


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
    wb_obj = openpyxl.load_workbook(path)
    sheet_obj = wb_obj.active
    m_row = sheet_obj.max_row

    result = {}

    for i in range(start_row, m_row + 1):
        scale = sheet_obj.cell(row=i, column=scales_column).value

        # A new scale appears
        if scale not in result and scale is not None:
            current_scale = scale
            result[scale] = {"queries": [], "codes": [], "related_scales": []}  # codes empty: no other scales related

        # For merged cells
        if scale is None:
            scale = current_scale

        query = sheet_obj.cell(row=i, column=queries_column).value
        queries = []

        related_code = str(sheet_obj.cell(row=i, column=related_column).value)
        codes = []
        if related_code is not None:
            if "," in related_code:
                codes = related_code.split(",")
            result[scale]["codes"] = [related_code] if len(codes) == 0 else codes
            result[scale]["related_scales"] = get_related_scales(wb_obj, related_code, start_row, scales_column,
                                                                 related_column)

        if query is not None:
            # Multiple queries in the same cell
            if "," in query:
                queries = query.split(",")
            if len(queries) > 0:
                for q in queries:
                    result[scale]["queries"].append(q.strip())
            # Single query
            else:
                result[scale]["queries"].append(query.strip())

    logger.debug("Scales and queries loaded")

    return result


def get_related_scales(wb_obj, code: int, start_row: int, scales_column: int, related_column: int):
    """
    Function that returns the name of all the scales related to the relationship code given

    :param wb_obj: the current Excel workbook
    :param code: int - relationship code
    :param start_row: int - row number where the first query is located
    :param scales_column: int - column number where the scales are located
    :param related_column: int - column number where the relationship between queries in different scales is located
    :return: result: list - the list with the scale names

    """

    sheet_obj = wb_obj.active
    m_row = sheet_obj.max_row

    result = []

    for i in range(start_row, m_row + 1):
        related_code = sheet_obj.cell(row=i, column=related_column).value
        if related_code is not None and related_code == code:
            scale = sheet_obj.cell(row=i, column=scales_column).value
            while scale is None:
                for j in range(i, start_row, -1):
                    scale = sheet_obj.cell(row=j, column=scales_column).value
            result.append(scale)

    return result


queries_and_scales = get_queries_and_scales("./excel/scales.xlsx", 5, 2, 4, 5)
print(queries_and_scales)

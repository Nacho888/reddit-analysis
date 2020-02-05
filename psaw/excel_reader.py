import openpyxl


def get_queries_and_scales(path):
    """ Function to obtain the scales' names and queries

            Parameters:
                path -- the path where the excel spreadsheet is located
            Returns:
                result -- a dictionary with scales as keys and list of queries as values
        """

    # Workbook object is created
    wb_obj = openpyxl.load_workbook(path)
    sheet_obj = wb_obj.active
    m_row = sheet_obj.max_row

    result = {}

    # From row [5 - end]
    # Column 2 (B) stores scale names, column 4 (D) stores queries
    for i in range(5, m_row + 1):
        scale = sheet_obj.cell(row=i, column=2).value

        if scale not in result and scale is not None:
            current_scale = scale
            result[scale] = []

        if scale is None:
            scale = current_scale

        query = sheet_obj.cell(row=i, column=4).value
        result[scale].append(query)

    return result

import openpyxl


def get_queries_and_scales(path):
    """ Function to obtain the scales' names and queries

            Parameters:
                path -- string
                    the path where the excel spreadsheet is located
            Returns:
                result -- dict
                    a dictionary with scales as keys and list of queries as values
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

        # A new scale appears
        if scale not in result and scale is not None:
            current_scale = scale
            result[scale] = []

        # For merged cells
        if scale is None:
            scale = current_scale

        query = sheet_obj.cell(row=i, column=4).value
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

    return result

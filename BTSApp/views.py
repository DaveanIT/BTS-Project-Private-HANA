from django.shortcuts import render
from django.conf import settings
from hdbcli import dbapi

def connect_to_hana():
    """
    Establishes a connection to SAP HANA using hdbcli.
    Returns the connection object.
    """
    config = settings.SAP_HANA_CONFIG
    try:
        # Connect to SAP HANA
        hana_connection = dbapi.connect(
            address=config['HOST'],
            port=config['PORT'],
            user=config['USER'],
            password=config['PASSWORD'],
        )
        print("Connection to SAP HANA successful!")
        
        return hana_connection
    except dbapi.Error as e:
        print(f"Error connecting to SAP HANA: {e}")
        raise
    
def index(request):
    data = {
        "bills": None,  # Data from first stored procedure
        "status": None,  # Data from second stored procedure
    }
    
    if request.method == 'POST':
        leg_no = request.POST.get('legno', 'n')  # Default to 'n' if unchecked
        approved = request.POST.get('approved', 'n')  # Default to 'n' if unchecked
        vetoed = request.POST.get('vetoed', 'n')  # Default to 'n' if unchecked
        ovrd = request.POST.get('overrdn', 'n')  # Default to 'n' if unchecked
        assigned = request.POST.get('toassgnd', 'n')  # Default to 'n' if unchecked
        intro = request.POST.get('tointro', 'n')  # Default to 'n' if unchecked
        senator = request.POST.get('tosnt', 'n')  # Default to 'n' if unchecked
        prim_spon = request.POST.get('primspon', 'n')  # Default to 'n' if unchecked
        to_gov = request.POST.get('togov', 'n')  # Default to 'n' if unchecked
        to_lt_gov = request.POST.get('toltgov', 'n')  # Default to 'n' if unchecked
        from_dt = request.POST.get('fromdate', 'n')  # Default to 'n' if unchecked
        to_dt = request.POST.get('todate', 'n')  # Default to 'n' if unchecked
        billno = request.POST.get('billnum', 'n')  # Default to 'n' if unchecked
        actnum = request.POST.get('actno', 'n')  # Default to 'n' if unchecked
        brno = request.POST.get('brno', 'n')  # Default to 'n' if unchecked
        amnno = request.POST.get('amnno', 'n')  # Default to 'n' if unchecked
        resono = request.POST.get('resono', 'n')  # Default to 'n' if unchecked
        goverrno = request.POST.get('govrrno', 'n')  # Default to 'n' if unchecked
    
        try:
            # Connect to SAP HANA
            hana_connection = connect_to_hana()

            # Use a cursor to execute the procedure
            with hana_connection.cursor() as cursor:
                # Get the dbname from the settings
                dbname = settings.SAP_HANA_CONFIG['DBNAME']
                
                # Set the schema dynamically using dbname from settings
                cursor.execute(f"SET SCHEMA {dbname}")

                # Call the stored procedure with parameters
                procedure = "CALL QSYS_BTS_PageLoad (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(procedure, (1, 'N', leg_no, approved, vetoed, ovrd, assigned, intro, senator, to_gov, to_lt_gov, from_dt, to_dt, billno, actnum, brno, amnno, resono, goverrno, ''))

                result1 = cursor.fetchall()
                columns1 = [column[0] for column in cursor.description]
                result_data1 = [dict(zip(columns1, row)) for row in result1]  # Convert the result into dictionaries

                # Call the stored procedure with parameters
                procedure = "CALL QSYS_BTS_PageLoad (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(procedure, (2, 'n', leg_no, approved, vetoed, ovrd, assigned, intro, senator, to_gov, to_lt_gov, from_dt, to_dt, billno, actnum, brno, amnno, resono, goverrno, ''))

                result2 = cursor.fetchall()
                columns2 = [column[0] for column in cursor.description]
                result_data2 = [dict(zip(columns2, row)) for row in result2]

                # Close the database connection
                hana_connection.close()

                # Prepare the data for the template
                data = {
                    "bills": result_data1,  # Data from first stored procedure
                    "status": result_data2,  # Data from second stored procedure
                }

        except dbapi.Error as e:
            results = {'error': f"Error executing stored procedure: {e}"}
        finally:
            # Close the connection if it was established
            if 'hana_connection' in locals() and hana_connection:
                hana_connection.close()

    else:
        try:
            # Connect to SAP HANA
            hana_connection = connect_to_hana()

            # Use a cursor to execute the procedure
            with hana_connection.cursor() as cursor:
                # Get the dbname from the settings
                dbname = settings.SAP_HANA_CONFIG['DBNAME']
                
                # Set the schema dynamically using dbname from settings
                cursor.execute(f"SET SCHEMA {dbname}")

                # Call the stored procedure with parameters
                procedure = "CALL QSYS_BTS_PageLoad (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(procedure, (1, 'Y', leg_no, approved, vetoed, ovrd, assigned, intro, senator, to_gov, to_lt_gov, from_dt, to_dt, billno, actnum, brno, amnno, resono, goverrno, ''))

                result1 = cursor.fetchall()
                columns1 = [column[0] for column in cursor.description]
                result_data1 = [dict(zip(columns1, row)) for row in result1]  # Convert the result into dictionaries

                # Call the stored procedure with parameters
                procedure = "CALL QSYS_BTS_PageLoad (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(procedure, (2, 'Y', leg_no, approved, vetoed, ovrd, assigned, intro, senator, to_gov, to_lt_gov, from_dt, to_dt, billno, actnum, brno, amnno, resono, goverrno, ''))

                result2 = cursor.fetchall()
                columns2 = [column[0] for column in cursor.description]
                result_data2 = [dict(zip(columns2, row)) for row in result2]

                # Close the database connection
                hana_connection.close()

                # Prepare the data for the template
                data = {
                    "bills": result_data1,  # Data from first stored procedure
                    "status": result_data2,  # Data from second stored procedure
                }

        except dbapi.Error as e:
            results = {'error': f"Error executing stored procedure: {e}"}
        finally:
            # Close the connection if it was established
            if 'hana_connection' in locals() and hana_connection:
                hana_connection.close()

    # Render the template with the data from the stored procedure
    return render(request, "index.html", data)


def stored_procedure_view(request):
    """
    View to execute a stored procedure in SAP HANA with parameters.
    """

    try:
        # Connect to SAP HANA
        hana_connection = connect_to_hana()

        # Use a cursor to execute the procedure
        with hana_connection.cursor() as cursor:
            # Get the dbname from the settings
            dbname = settings.SAP_HANA_CONFIG['DBNAME']
            
            # Set the schema dynamically using dbname from settings
            cursor.execute(f"SET SCHEMA {dbname}")

            # Call the stored procedure with parameters
            procedure = "CALL QSYS_BTS_SponList(?, ?)"
            cursor.execute(procedure, (param1, param2))

            # Fetch the results if the procedure returns a result set
            if cursor.description:
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                results = {'columns': column_names, 'rows': rows}
                print(results)
            else:
                results = {'message': 'Procedure executed successfully, no result set returned.'}

    except dbapi.Error as e:
        results = {'error': f"Error executing stored procedure: {e}"}
    finally:
        # Close the connection if it was established
        if 'hana_connection' in locals() and hana_connection:
            hana_connection.close()

    # Pass results to the template
    return render(request, 'test.html', {'results': results})

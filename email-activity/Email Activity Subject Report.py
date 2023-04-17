#pip install --user --upgrade "sqlalchemy<2.0"

import json
import psycopg2
import pandas as pd
import sqlalchemy as sa
import sys
import warnings
from styleframe import StyleFrame, Styler, utils

try:
    if (len (sys.argv) != 2):
        print("\nWRONG ARGUMENTS ENTERED\tUsage python 'Email Activity Subject Report' 'InputFileName'")
        exit ()  
        
    else:

        def Input_file_parser():

            """ 
            This function parses the Input Configuration JSON File 

            __docstring__

            The intention of creating this file is that the user can login with their own account and could also modify the 
            filtering condition dynamically

            """
            with open(sys.argv[1], 'r') as config_Customer_Email_Analysis:
                data=config_Customer_Email_Analysis.read()

            obj = json.loads(data)

            global user_name
            global password
            global filter_system_modstamp

            user_name = str(obj['user_name'])
            password = str(obj['password'])
            filter_system_modstamp = obj['filter_system_modstamp']

        def connection():

            """ This connects to the RedShift Data Warehouse """

            try:
                global prod_con
                prod_con = sa.create_engine(f'postgresql+psycopg2://{user_name}:{password}@llp-data-warehouse-prod-us-west-2.cmilkngqqmcl.us-west-2.redshift.amazonaws.com:5439/warehouse_prod')

                print('Successfully Connected to Production Datawarehouse. Fetching further details ..........')
            except:
                print("Not connected to VPN or not having access to warehouse production data.........")

        def SQL_to_DataFrame():

            """ 
            This converts SQL Script output to Pandas DataFrame object
            Manipulating DataFrame would be efficient and faster than performing manipulations on the SQL script rather

            """
            global df
            df = pd.read_sql_query(f"SELECT t.id,t.who_id,t.account_id,a.name AS account_name,t.subject,t.created_date,t.system_modstamp AS task_update_timestamp,t.description AS email_body,t.status,t.owner_id,a.is_live_on_logistics_c,a.is_live_on_marketplace_c,a.is_live_on_payments_c,a.customer_segment_c,a.this_quarter_gmv_c FROM salesforce.task t JOIN salesforce.account a ON t.account_id = a.id WHERE 1=1 AND t.type = 'Email' and t.system_modstamp > '{filter_system_modstamp}'",prod_con)

        #     df = pd.read_sql_query(f"select * FROM salesforce.task limit 60",prod_con)
            return df

        def product_lines_categorization():

            """

            Categorize Product Lines into marketplace, payments and logistics

            """
            global marketplace
            global payments
            global logistics 
            global Marketplace_Price_Increase
            global Past_Due
            global Request
            global Problem
            global Question

            marketplace= ['license','menu','integration','CRM','reporting','credits','sku','download','quickbooks','metrc integration'] 
            payments= ['invoice','LLF','fee','servicing-payments@leaflink','extend','leaflink financial','bank account','debit','financials','credit limit']
            logistics= ['box','return','warehouse','pickup','manifest','pick up','totes','reschedule','COD','tote']
            Marketplace_Price_Increase= ['rate', 'subscription', 'price increase', 'cost']
            Past_Due= ['outstanding balance', 'outstanding bill', 'overdue', 'past due']
            Request = ['request', 'feature', 'addition']
            Problem = ['problem', 'issue', 'bug', 'challenge']
            Question = ['question', 'how to', 'when is', 'where are', 'where is', 'why are', 'why is', 'who are', 'who is']

            global all_product_lines
            all_product_lines= marketplace + payments + logistics

        def DataFrame_segmented():

            """
            This classify the raw data into product lines granuality and displays daat in the new column of the raw data

            """

            global df_segmented
            df_segmented= df
            df_segmented["Product_line_Granuality"] = df_segmented['subject'].apply(lambda x: 'Marketplace, Payments or Logistics' if any(i in x for i in all_product_lines) else '')
            return df_segmented

        def product_lines(product_line):

            """
            Summarizes report about Product Lines

            """
            empty_list= []

            for entity in product_line:
                lst_product_line= entity,len(df.loc[df['subject'].str.contains(entity, case=False)].index)
                empty_list.append(lst_product_line)

            if product_line == marketplace:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'marketplace',1:'Count'})
                return lst_product_line_df
            elif product_line == payments:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'payments',1:'Count'})
                return lst_product_line_df
            elif product_line == logistics:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'logistics',1:'Count'})
                return lst_product_line_df
            elif product_line == Marketplace_Price_Increase:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'Marketplace Price Increase',1:'Count'})
                return lst_product_line_df
            elif product_line == Past_Due:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'Past Due',1:'Count'})
                return lst_product_line_df
            elif product_line == Request:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'Request',1:'Count'})
                return lst_product_line_df
            elif product_line == Problem:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'Problem',1:'Count'})
                return lst_product_line_df
            elif product_line == Question:
                lst_product_line_df= pd.DataFrame(empty_list).rename(columns = {0:'Question',1:'Count'})
                return lst_product_line_df
            else:
                pass

        def summary():

            """
            Exports raw data & summary report to an automated csv_file

            """
            global summary_attributes

            summary_attributes= ['Total # of emails','Companies / Subject Lines','Marketplace Index','Payments Index','Logistics Index','Marketplace Price Increase','Past Due (Marketplace Deactivation)','Request','Problem','Question']

            summary_attributes_0= pd.concat([pd.DataFrame([summary_attributes[0]]), pd.DataFrame([len(df.index)])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})
            summary_attributes_1= pd.concat([pd.DataFrame([summary_attributes[1]]), pd.DataFrame([str(len(pd.unique(df['subject']))) +' unique subject lines'+str(' / ')+ str(df['account_id'].nunique()) +' Customers'])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})
            summary_attributes_2= pd.concat([pd.DataFrame([summary_attributes[2]]), pd.DataFrame([product_lines_marketplace['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})
            summary_attributes_3= pd.concat([pd.DataFrame([summary_attributes[3]]), pd.DataFrame([product_lines_payments['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})
            summary_attributes_4= pd.concat([pd.DataFrame([summary_attributes[4]]), pd.DataFrame([product_lines_logistics['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})
            summary_attributes_5= pd.concat([pd.DataFrame([summary_attributes[5]]), pd.DataFrame([product_lines_Marketplace_Price_Increase['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})
            summary_attributes_6= pd.concat([pd.DataFrame([summary_attributes[6]]), pd.DataFrame([product_lines_Past_Due['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Summary',1:'Stats'})

            summary_overall= pd.concat([summary_attributes_0, summary_attributes_1, summary_attributes_2, summary_attributes_3, summary_attributes_4, summary_attributes_5, summary_attributes_6], ignore_index=True)
            return summary_overall

        def sentiment():

            """
            Concludes the sentiment of the email based on the subject lines

            """

            sentiment_attributes_7= pd.concat([pd.DataFrame([summary_attributes[7]]), pd.DataFrame([product_lines_Request['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Sentiment',1:'Counts'})
            sentiment_attributes_8= pd.concat([pd.DataFrame([summary_attributes[8]]), pd.DataFrame([product_lines_Problem['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Sentiment',1:'Counts'})
            sentiment_attributes_9= pd.concat([pd.DataFrame([summary_attributes[9]]), pd.DataFrame([product_lines_Question['Count'].sum(axis=0)])],ignore_index=True).transpose().rename(columns = {0:'Sentiment',1:'Counts'})

            sentiment_overall= pd.concat([sentiment_attributes_7, sentiment_attributes_8, sentiment_attributes_9], ignore_index=True)
            return sentiment_overall

        def export_to_csv():

            """
            Exports raw data & summary report to an automated csv_file

            """
            global startDate
            global endDate

            import datetime
            endDate= datetime.date.today().strftime("%B %d, %Y")

            from datetime import datetime
            startDate= datetime.strptime(filter_system_modstamp, '%Y-%m-%d').strftime("%B %d, %Y")

            try:

                # To remove warnings for 'writer.save()' which could be removed later by python community
                warnings.filterwarnings("ignore")
                global writer

        #         writer = pd.ExcelWriter(startDate+"-"+endDate+" email activity subject report.xlsx")
                writer = StyleFrame.ExcelWriter(startDate+"-"+endDate+" email activity subject report.xlsx")

                summary_style=StyleFrame(summary)
                summary_style.apply_column_style(cols_to_style = summary.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                summary_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                summary_style.set_column_width(['Summary','Stats'], 35)
                summary_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=1, index= False)

                product_lines_marketplace_style=StyleFrame(product_lines_marketplace)
                product_lines_marketplace_style.apply_column_style(cols_to_style = product_lines_marketplace.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                product_lines_marketplace_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                product_lines_marketplace_style.set_column_width(['marketplace','Count'], 35)
                product_lines_marketplace_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=5, index= False)

                product_lines_payments_style=StyleFrame(product_lines_payments)
                product_lines_payments_style.apply_column_style(cols_to_style = product_lines_payments.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                product_lines_payments_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                product_lines_payments_style.set_column_width(['payments','Count'], 35)
                product_lines_payments_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=8, index= False)

                product_lines_logistics_style=StyleFrame(product_lines_logistics)
                product_lines_logistics_style.apply_column_style(cols_to_style = product_lines_logistics.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                product_lines_logistics_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                product_lines_logistics_style.set_column_width(['logistics','Count'], 35)
                product_lines_logistics_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=11, index= False)

                sentiment_style=StyleFrame(sentiment)
                sentiment_style.apply_column_style(cols_to_style = sentiment.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                sentiment_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                sentiment_style.set_column_width(['Sentiment','Counts'], 35)
                sentiment_style.to_excel(writer, sheet_name='Summary',startrow=15, startcol=1, index= False)

                df_style=StyleFrame(df_segmented)
                df_style.apply_column_style(cols_to_style = df_segmented.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                df_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.white, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                df_style.set_column_width(['id', 'who_id', 'account_id', 'account_name', 'subject', 'created_date',
                       'task_update_timestamp', 'email_body', 'status', 'owner_id',
                       'is_live_on_logistics_c', 'is_live_on_marketplace_c',
                       'is_live_on_payments_c', 'customer_segment_c', 'this_quarter_gmv_c', 'Product_line_Granuality'], 25)
                df_style.to_excel(writer, sheet_name='Full Raw Data',index= False)


                writer.save()
                print("\n**************  Report Successfully generated  **************")

            except:
                print("File '__ email activity subject report.xlsx' is opened in the background. Please close the file and re-run the script")

        # Calling Functions

        Input_file_parser()
        connection()
        SQL_to_DataFrame()
        product_lines_categorization()
        DataFrame_segmented()
        product_lines_marketplace = product_lines(marketplace)
        product_lines_payments = product_lines(payments)
        product_lines_logistics = product_lines(logistics)
        product_lines_Marketplace_Price_Increase = product_lines(Marketplace_Price_Increase)
        product_lines_Past_Due = product_lines(Past_Due)
        product_lines_Request = product_lines(Request)
        product_lines_Problem = product_lines(Problem)
        product_lines_Question = product_lines(Question)
        summary= summary()
        sentiment= sentiment()
        export_to_csv()

except:
    print("\nINVALID INPUTS\n")


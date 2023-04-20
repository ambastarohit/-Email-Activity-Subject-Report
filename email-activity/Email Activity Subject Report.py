#pip install --user --upgrade "sqlalchemy<2.0"

import json
import psycopg2
import pandas as pd
import numpy as np
import sqlalchemy as sa
import warnings
import sys
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

            marketplace= ['license','menu','integration','CRM','reporting','credits','sku','download','quickbooks','metrc integration','Metrc'] 
            payments= ['invoice', 'invoicing','LLF','fee','fees', 'servicing-payments@leaflink','extend','leaflink financial','bank account','debit','financials','credit limit']
            logistics= ['box','return','warehouse','pickup','manifest','pick up','totes','reschedule','COD','tote']
            Marketplace_Price_Increase= ['rate', 'subscription', 'price increase', 'cost']
            Past_Due= ['outstanding balance', 'outstanding bill', 'overdue', 'past due']
            Request = ['request', 'feature', 'addition']
            Problem = ['problem', 'issue', 'bug', 'challenge']
            Question = ['question', 'how to', 'when is', 'where are', 'where is', 'why are', 'why is', 'who are', 'who is']

            global m_p
            global p_l
            global m_l

            m_p= marketplace + payments
            p_l= payments + logistics
            m_l= marketplace + logistics

            global all_product_lines
            all_product_lines= marketplace + payments + logistics

        def Product_granular_classification():

            """
            This classifies the email to the lowest granuality level in the 'Full data' csv file (i.e. entity for each product line. Ex- 'license','menu' etc.)

            This function still need to be automated. So below four lines are commented for automating this.

            """
            # for entity in marketplace:
            #     locked_data_entity = df.loc[df['subject'].str.contains(entity, case=False)]
            #     locked_data_entity[entity]= entity
            #     gt= pd.concat([license, locked_data_entity])

            license= df.loc[df['subject'].str.contains('license', case=False)] 
            license['license']= 'license'

            menu= df.loc[df['subject'].str.contains('menu', case=False)] 
            menu['menu']= 'menu'

            integration= df.loc[df['subject'].str.contains('integration', case=False)] 
            integration['integration']= 'integration'

            CRM= df.loc[df['subject'].str.contains('CRM', case=False)] 
            CRM['CRM']= 'CRM'

            reporting= df.loc[df['subject'].str.contains('reporting', case=False)] 
            reporting['reporting']= 'reporting'

            credits= df.loc[df['subject'].str.contains('credits', case=False)] 
            credits['credits']= 'credits'

            sku= df.loc[df['subject'].str.contains('sku', case=False)] 
            sku['sku']= 'sku'

            download= df.loc[df['subject'].str.contains('download', case=False)] 
            download['download']= 'download'

            quickbooks= df.loc[df['subject'].str.contains('quickbooks', case=False)] 
            quickbooks['quickbooks']= 'quickbooks'

            metrc_integration= df.loc[df['subject'].str.contains('metrc integration', case=False)] 
            metrc_integration['metrc_integration']= 'metrc_integration'

            Metrc= df.loc[df['subject'].str.contains('Metrc', case=False)] 
            Metrc['Metrc']= 'Metrc'

            invoice= df.loc[df['subject'].str.contains('invoice', case=False)] 
            invoice['invoice']= 'invoice'

            invoicing= df.loc[df['subject'].str.contains('invoicing', case=False)] 
            invoicing['invoicing']= 'invoicing'

            LLF= df.loc[df['subject'].str.contains('LLF', case=False)] 
            LLF['LLF']= 'LLF'

            fee= df.loc[df['subject'].str.contains('fee', case=False)] 
            fee['fee']= 'fee'

            fees= df.loc[df['subject'].str.contains('fees', case=False)] 
            fees['fees']= 'fees'

            servicing_payments_leaflink= df.loc[df['subject'].str.contains('servicing-payments@leaflink', case=False)] 
            servicing_payments_leaflink['servicing_payments_leaflink']= 'servicing-payments@leaflink'

            extend= df.loc[df['subject'].str.contains('extend', case=False)] 
            extend['extend']= 'extend'

            leaflink_financial= df.loc[df['subject'].str.contains('leaflink financial', case=False)] 
            leaflink_financial['leaflink_financial']= 'leaflink financial'

            bank_account= df.loc[df['subject'].str.contains('bank account', case=False)] 
            bank_account['bank_account']= 'bank account'

            debit= df.loc[df['subject'].str.contains('debit', case=False)] 
            debit['debit']= 'debit'

            financials= df.loc[df['subject'].str.contains('financials', case=False)] 
            financials['financials']= 'financials'

            credit_limit= df.loc[df['subject'].str.contains('credit limit', case=False)] 
            credit_limit['credit_limit']= 'credit limit'

            box= df.loc[df['subject'].str.contains('box', case=False)] 
            box['box']= 'box'

            return1= df.loc[df['subject'].str.contains('return', case=False)] 
            return1['return1']= 'return'

            warehouse= df.loc[df['subject'].str.contains('warehouse', case=False)] 
            warehouse['warehouse']= 'warehouse'

            pickup= df.loc[df['subject'].str.contains('pickup', case=False)] 
            pickup['pickup']= 'pickup'

            manifest= df.loc[df['subject'].str.contains('manifest', case=False)] 
            manifest['manifest']= 'manifest'

            pick_up= df.loc[df['subject'].str.contains('pick up', case=False)] 
            pick_up['pick_up']= 'pick up'

            totes= df.loc[df['subject'].str.contains('totes', case=False)] 
            totes['totes']= 'totes'

            reschedule= df.loc[df['subject'].str.contains('reschedule', case=False)] 
            reschedule['reschedule']= 'reschedule'

            COD= df.loc[df['subject'].str.contains('COD', case=False)] 
            COD['COD']= 'COD'

            tote= df.loc[df['subject'].str.contains('tote', case=False)] 
            tote['tote']= 'tote'



            df_pro_line = pd.concat([license, menu, integration, CRM, reporting, credits, sku, download, quickbooks,metrc_integration, Metrc, invoice, invoicing, LLF,fee, fees, servicing_payments_leaflink, extend, leaflink_financial, bank_account, debit, financials, credit_limit, box, return1, warehouse, pickup, manifest, pick_up,  totes, reschedule,COD, tote ])
            df_pro_line= df_pro_line[['id', 'license','menu','integration','CRM','reporting','credits','sku','download','quickbooks','metrc_integration','Metrc','invoice','invoicing','LLF','fee','fees','servicing_payments_leaflink','extend','leaflink_financial','bank_account','debit','financials','credit_limit','box','return1','warehouse','pickup','manifest','pick_up','totes','reschedule','COD','tote']]
            df_pro_line['All']= df_pro_line['license'].replace(np. nan,'',regex=True)+ df_pro_line['menu'].replace(np. nan,'',regex=True)+ df_pro_line['integration'].replace(np. nan,'',regex=True)+df_pro_line['CRM'].replace(np. nan,'',regex=True)+df_pro_line['reporting'].replace(np. nan,'',regex=True)+df_pro_line['credits'].replace(np. nan,'',regex=True)+df_pro_line['sku'].replace(np. nan,'',regex=True)+df_pro_line['download'].replace(np. nan,'',regex=True)+df_pro_line['quickbooks'].replace(np. nan,'',regex=True)+df_pro_line['metrc_integration'].replace(np. nan,'',regex=True)+df_pro_line['Metrc'].replace(np. nan,'',regex=True)+df_pro_line['invoice'].replace(np. nan,'',regex=True)+df_pro_line['invoicing'].replace(np. nan,'',regex=True)+df_pro_line['LLF'].replace(np. nan,'',regex=True)+df_pro_line['fee'].replace(np. nan,'',regex=True)+df_pro_line['fees'].replace(np. nan,'',regex=True)+df_pro_line['servicing_payments_leaflink'].replace(np. nan,'',regex=True)+df_pro_line['extend'].replace(np. nan,'',regex=True)+df_pro_line['leaflink_financial'].replace(np. nan,'',regex=True)+df_pro_line['bank_account'].replace(np. nan,'',regex=True)+df_pro_line['debit'].replace(np. nan,'',regex=True)+df_pro_line['financials'].replace(np. nan,'',regex=True)+df_pro_line['credit_limit'].replace(np. nan,'',regex=True)+df_pro_line['box'].replace(np. nan,'',regex=True)+df_pro_line['return1'].replace(np. nan,'',regex=True)+df_pro_line['warehouse'].replace(np. nan,'',regex=True)+df_pro_line['pickup'].replace(np. nan,'',regex=True)+df_pro_line['manifest'].replace(np. nan,'',regex=True)+df_pro_line['pick_up'].replace(np. nan,'',regex=True)+df_pro_line['totes'].replace(np. nan,'',regex=True)+df_pro_line['reschedule'].replace(np. nan,'',regex=True)+df_pro_line['COD'].replace(np. nan,'',regex=True)+df_pro_line['tote'].replace(np. nan,'',regex=True) 

            merged_df= pd.merge(df, df_pro_line, how="left", on ="id")

            global merged_dataframe

            merged_dataframe= merged_df[['id', 'who_id', 'account_id', 'account_name', 'subject', 'created_date',
                   'task_update_timestamp', 'email_body', 'status', 'owner_id',
                   'is_live_on_logistics_c', 'is_live_on_marketplace_c',
                   'is_live_on_payments_c', 'customer_segment_c', 'this_quarter_gmv_c', 'All']]
            return merged_dataframe

        def final_df():

            """
            This classifies the email to the product lines level in the 'Full data' csv file

            """
            def flag_merged_dataframe(merged_dataframe):

                if (merged_dataframe['All'] in marketplace):
                    return 'Marketplace'
                elif (merged_dataframe['All'] in payments):
                    return 'Payments'
                elif (merged_dataframe['All'] in logistics):
                    return 'Logistics'
                else:
                    return np.nan 

        #     global merged_dataframe

            merged_dataframe['Product Line'] = merged_dataframe.apply(flag_merged_dataframe, axis = 1)
            global merged_fin_df
            merged_fin_df = merged_dataframe
            return merged_fin_df

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

                # To remove warnings for 'writer.save()' which is to be removed later by python community
                warnings.filterwarnings("ignore")
                global writer

                writer = StyleFrame.ExcelWriter(startDate+"-"+endDate+" email activity subject report.xlsx")

        #         Generates 'Summary' report in Excel

                summary_style=StyleFrame(summary)
                summary_style.apply_column_style(cols_to_style = summary.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                summary_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                summary_style.set_column_width(['Summary','Stats'], 35)
                summary_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=1, index= False)

        #         Generates 'Maretplace' report in Excel

                product_lines_marketplace_style=StyleFrame(product_lines_marketplace)
                product_lines_marketplace_style.apply_column_style(cols_to_style = product_lines_marketplace.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                product_lines_marketplace_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                product_lines_marketplace_style.set_column_width(['marketplace','Count'], 35)
                product_lines_marketplace_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=5, index= False)

        #         Generates 'Payments' report in Excel

                product_lines_payments_style=StyleFrame(product_lines_payments)
                product_lines_payments_style.apply_column_style(cols_to_style = product_lines_payments.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                product_lines_payments_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                product_lines_payments_style.set_column_width(['payments','Count'], 35)
                product_lines_payments_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=8, index= False)

        #         Generates 'Logistics' report in Excel

                product_lines_logistics_style=StyleFrame(product_lines_logistics)
                product_lines_logistics_style.apply_column_style(cols_to_style = product_lines_logistics.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                product_lines_logistics_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                product_lines_logistics_style.set_column_width(['logistics','Count'], 35)
                product_lines_logistics_style.to_excel(writer, sheet_name='Summary',startrow=1, startcol=11, index= False)

        #         Generates 'Sentiments' report in Excel

                sentiment_style=StyleFrame(sentiment)
                sentiment_style.apply_column_style(cols_to_style = sentiment.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                sentiment_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.yellow, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                sentiment_style.set_column_width(['Sentiment','Counts'], 35)
                sentiment_style.to_excel(writer, sheet_name='Summary',startrow=15, startcol=1, index= False)

        #         Generates 'Full Raw Data' report in Excel

                df_style=StyleFrame(merged_fin_df)
                df_style.apply_column_style(cols_to_style = merged_fin_df.columns, styler_obj = Styler(bg_color=utils.colors.white, bold=False, font=utils.fonts.arial,font_size=9),style_header=True)
                df_style.apply_headers_style(styler_obj=Styler(bg_color=utils.colors.white, bold=True, font_size=10, font_color=utils.colors.black,number_format=utils.number_formats.general, protection=False))
                df_style.set_column_width(['id', 'who_id', 'account_id', 'account_name', 'subject', 'created_date',
                       'task_update_timestamp', 'email_body', 'status', 'owner_id', 'is_live_on_logistics_c',
                       'is_live_on_marketplace_c','is_live_on_payments_c', 'customer_segment_c', 'this_quarter_gmv_c','All', 'Product Line'], 30)

                df_style.set_row_height(rows=df_style.row_indexes, height=15) 
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
        warnings.filterwarnings("ignore")
        Product_granular_classification()
        warnings.filterwarnings("ignore")
        final_df()
        warnings.filterwarnings("ignore")
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
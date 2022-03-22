"""To run, use the command line arguments in the order:
    bounding date, jira user, jira access token, zendesk user, zendesk API token"""

import requests
import pandas as pd 
import json
import pandas.io.json as pd_json
from urllib.parse import urlencode
import sys
from datetime import datetime
import http.client
import numpy as np

def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except http.client.IncompleteRead as e:
            return e.partial
    return inner

http.client.HTTPResponse.read = patch_http_response_read(http.client.HTTPResponse.read)

class JiraDataCollection:
    '''Class to collect jira data for all issues associated to Zendesk tickets'''
    def __init__(self, access_token, user):
        '''Constructor function for class JiraDataCollection'''
        # Access token is the Jira API token for the Jira account associated with the user
        self.access_token = access_token
        self.user = user

        self.issue_url = 'https://qubole.atlassian.net/rest/api/3/issue/'
        #print("JiraDataCollection Class: Constructor")
        # self.headers = {
        #   'Accept': 'application/json',
        #   'Bearer': self.access_token   
        # }

        self.users = set()
        self.jira_details = pd.DataFrame()
        self.user_details = pd.DataFrame()
        self.jira_comments = pd.DataFrame()
        self.jira_user_columns = ['jira_user_id', 'accountType', 'active','displayName',  'emailAddress', 'user_id', 'name', 'self', 'timeZone']
       # self.jira_details_columns = ['Assignee', 'Created', 'Creator', '[CHART]_Time_in_Status', '[CHART]_Date_of_First_Response', 'Flagged', 'Business_Value', 'Release_Version_History', 'Sprint', 'Customers_Affected', 'Release_Version', 'Release_Version_id', 'Release_Version_self', 'Release_Version_value',  'Epic_Link', 'Epic_Name', 'Epic_Status', 'Epic_Status_id',  'Epic_Status_self', 'Epic_Status_value', 'Workaround/_Resolution', 'Release_Notes', 'Release_Notes_content', 'Release_Notes_type', 'Release_Notes_version', 'Component', 'Internal_Impact', 'Internal_Impact_content','Doc_Impact', 'Doc_Impact_id', 'Doc_Impact_self', 'Doc_Impact_value', 'Parent_Link_showField', 'Development', 'Team', 'Team_id', 'Team_title', 'Affected_Clouds', 'Organizations', 'Approved_By', 'Approved_By_accountId', 'Approved_By_accountType', 'Approved_By_active', 'Approved_By_avatarUrls_16x16', 'Approved_By_avatarUrls_24x24', 'Approved_By_avatarUrls_32x32', 'Approved_By_avatarUrls_48x48',   'Approved_By_displayName', 'Approved_By_emailAddress', 'Approved_By_key', 'Approved_By_name', 'Approved_By_self', 'Approved_By_timeZone', 'Affects_Version/s', 'Request_Type', 'Need_Access_To', 'Environments', 'ssh-pub-key','ssh-pub-key_content','ssh-pub-key_type','ssh-pub-key_version','unix-password-hash','Need-Access-To','Approver','Approver_accountId','Approver_accountType','Approver_active','Approver_avatarUrls_16x16','Approver_avatarUrls_24x24','Approver_avatarUrls_32x32','Approver_avatarUrls_48x48','Approver_displayName','Approver_emailAddress','Approver_key','Approver_name','Approver_self','Approver_timeZone','Release_Type','Release_Type_id','Release_Type_self','Release_Type_value','HR_INFO','Custom_Package/Custom_AMI/Node_Bootstrap_Name','Customers_Deployed','Zendesk_Ticket_IDs','ticket_ids','Zendesk_Ticket_IDs_type','Zendesk_Ticket_IDs_version','Zendesk_Ticket_Count','Component','Incident_Reported_Time','Incident_Resolution_Time','Test_Plan','Test_Plan_content','Test_Plan_type','Test_Plan_version','Review_Comments','Review_Comments_content','Review_Comments_type','Review_Comments_version','Request_participants','Satisfaction','Satisfaction_date','Environment','Affected_Environment','Found_in_Version','Found_in_Version_id','Found_in_Version_self','Found_in_Version_value','Flag','Count','Verification_Phase','Verification_Phase_id','Verification_Phase_self','Verification_Phase_value','Verified_Clouds','Offer_Acceptance_Date','Email','Phone','Manager','Date','Process_Type','Name','Contract_Type','Position','Department/Team','Location','Access_To','Access_To_id','Access_To_self','Access_To_value','Development_Phase','Automated','Automated_id','Automated_self','Automated_value','Effort_Spent','Effort_Spent_id','Effort_Spent_self','Effort_Spent_value','Production_Environments','Effort_Classification','Effort_Classification_id','Effort_Classification_self','Effort_Classification_value','Ops_Review_Required','Ops_Review_Required_id','Ops_Review_Required_self','Ops_Review_Required_value','Security_Impact','Security_Impact_id','Security_Impact_self','Security_Impact_value','productboard_URL','Start_date','Affected_Technologies','Goal','Persona','Audience_Size','customer_affected_count','Story_point_estimate', 'Availability','Availability_id','Availability_self','Availability_value','Launch_stage','Launch_stage_id','Launch_stage_self','Launch_stage_value','Feature_Setting','Feature_Setting_id','Feature_Setting_self','Feature_Setting_value','Feature_flag_name','Internal_release_note','Internal_release_note_content','Internal_release_note_type','Internal_release_note_version','Issue_color','Reviewer','Reviewer_accountId','Reviewer_accountType','Reviewer_active','Reviewer_avatarUrls_16x16','Reviewer_avatarUrls_24x24','Reviewer_avatarUrls_32x32','Reviewer_avatarUrls_48x48','Reviewer_displayName','Reviewer_emailAddress','Reviewer_key','Reviewer_name','Reviewer_self','Reviewer_timeZone','DOC_JIRA','Rollout_start_date','S1','S2','S3','S4','S5','Global','Rollback_date','Project_Manager','Technical_Lead','Product_Manager','Description','description_content','description_type','description_version','Due_date','Environment','environment_content','environment_type','environment_version','Fix_Version/s','Linked_Issues','issuetype_avatarId','issuetype_description','issuetype_iconUrl','issuetype_id','issuetype_name','issuetype_self','issuetype_subtask','jira_id','Labels','Last_Viewed','parent_fields_issuetype_avatarId','parent_fields_issuetype_description','parent_fields_issuetype_iconUrl','parent_fields_issuetype_id','parent_fields_issuetype_name','parent_fields_issuetype_self','parent_fields_issuetype_subtask','parent_fields_priority_iconUrl','parent_fields_priority_id','parent_fields_priority_name','parent_fields_priority_self','parent_fields_status_description','parent_fields_status_iconUrl','parent_fields_status_id','parent_fields_status_name','parent_fields_status_self','parent_fields_status_statusCategory_colorName','parent_fields_status_statusCategory_id','parent_fields_status_statusCategory_key','parent_fields_status_statusCategory_name','parent_fields_status_statusCategory_self','parent_fields_summary','parent_id','parent_key','parent_self','priority_iconUrl','priority_id','priority_name','priority_self','progress_percent','progress_progress','progress_total','project_avatarUrls_16x16','project_avatarUrls_24x24','project_avatarUrls_32x32','project_avatarUrls_48x48','project_id','project_key','project_name','project_projectCategory_description','project_projectCategory_id','project_projectCategory_name','project_projectCategory_self','project_projectTypeKey', 'project_self','Reporter','Resolution','resolution_description','resolution_id','resolution_name','resolution_self','Resolved','Security_Level','status_description','status_iconUrl','status_id','status_name','status_self','status_statusCategory_colorName','status_statusCategory_id','status_statusCategory_key','status_statusCategory_name','status_statusCategory_self','Status_Category_Changed','Sub-tasks','Summary','Remaining_Estimate','Original_Estimate','Time_Spent','timetracking_originalEstimate','timetracking_originalEstimateSeconds','timetracking_remainingEstimate','timetracking_remainingEstimateSeconds','timetracking_timeSpent','timetracking_timeSpentSeconds','Updated','Affects_Version/s','votes_hasVoted','votes_self','votes_votes','watches_isWatching','watches_self','watches_watchCount','worklog_maxResults','worklog_startAt','worklog_total','worklog_worklogs','Work_Ratio']
        self.jira_comments_columns = ['author', 'body_content', 'body_type', 'body_version', 'created', 'comment_id', 'jira_id', 'jsdPublic', 'self', 'updateAuthor_jira_user_id', 'updateAuthor_accountType', 'updateAuthor_active', 'updateAuthor_avatarUrls_16x16', 'updateAuthor_avatarUrls_24x24', 'updateAuthor_avatarUrls_32x32', 'updateAuthor_avatarUrls_48x48', 'updateAuthor_displayName', 'updateAuthor_emailAddress', 'updateAuthor_key', 'updateAuthor_name', 'updateAuthor_self', 'updateAuthor_timeZone', 'updated']
        self.jira_details_columns = ['Assignee', 'Component/s', 'Created', 'Creator', 'Flagged', 'Epic/Theme', 'Rank_(Obsolete)', 'Story_Points', 'Business_Value', 'Release_Version_History', 'Sprint', 'Customers_Affected', 'Release_Version', 'Release_Version_id', 'Release_Version_self', 'Release_Version_value', 'Epic_Link', 'Epic_Name', 'Epic_Status', 'Epic_Status_id', 'Epic_Status_self', 'Epic_Status_value', 'Epic_Colour', 'Rank', 'Phone_Screen_Score', 'Phone_Screen_Feedback', 'Workaround/_Resolution', 'Release_Notes', 'Release_Notes_content', 'Release_Notes_type', 'Release_Notes_version', 'Component', 'Internal_Impact', 'Internal_Impact_content', 'Internal_Impact_type', 'Internal_Impact_version', 'QA_Notes', 'QA_Notes_content', 'QA_Notes_type', 'QA_Notes_version', 'PR_Approval', 'QA_Approved', 'QA_Approved_id', 'QA_Approved_self', 'QA_Approved_value', 'Doc_Approved', 'Doc_Approved_id', 'Doc_Approved_self', 'Doc_Approved_value', 'Internal_Impact_Reviewed', 'Internal_Impact_Reviewed_id', 'Internal_Impact_Reviewed_self', 'Internal_Impact_Reviewed_value', 'QA_Engineer', 'QA_Engineer_accountId', 'QA_Engineer_accountType', 'QA_Engineer_active', 'QA_Engineer_avatarUrls_16x16', 'QA_Engineer_avatarUrls_24x24', 'QA_Engineer_avatarUrls_32x32', 'QA_Engineer_avatarUrls_48x48', 'QA_Engineer_displayName', 'QA_Engineer_emailAddress', 'QA_Engineer_key', 'QA_Engineer_name', 'QA_Engineer_self', 'QA_Engineer_timeZone', 'Approvals', 'Installation', 'Doc_Impact', 'Doc_Impact_id', 'Doc_Impact_self', 'Doc_Impact_value', 'Parent_Link_hasEpicLinkFieldDependency', 'Parent_Link_nonEditableReason_message', 'Parent_Link_nonEditableReason_reason', 'Parent_Link_showField', 'Development', 'Team', 'Team_id', 'Team_isShared', 'Team_title', 'Affected_Clouds', 'Organizations', 'Approved_By', 'Approved_By_accountId', 'Approved_By_accountType', 'Approved_By_active', 'Approved_By_avatarUrls_16x16', 'Approved_By_avatarUrls_24x24', 'Approved_By_avatarUrls_32x32', 'Approved_By_avatarUrls_48x48',   'Approved_By_displayName', 'Approved_By_emailAddress', 'Approved_By_key', 'Approved_By_name', 'Approved_By_self', 'Approved_By_timeZone', 'Affects_Version/s', 'Request_Type', 'Need_Access_To', 'Environments', 'ssh-pub-key','ssh-pub-key_content','ssh-pub-key_type','ssh-pub-key_version','unix-password-hash','Need-Access-To','Approver','Approver_accountId','Approver_accountType','Approver_active','Approver_avatarUrls_16x16','Approver_avatarUrls_24x24','Approver_avatarUrls_32x32','Approver_avatarUrls_48x48','Approver_displayName','Approver_emailAddress','Approver_key','Approver_name','Approver_self','Approver_timeZone','Release_Type','Release_Type_id','Release_Type_self','Release_Type_value','HR_INFO','Custom_Package/Custom_AMI/Node_Bootstrap_Name','Customers_Deployed','Zendesk_Ticket_IDs','ticket_ids','Zendesk_Ticket_IDs_type','Zendesk_Ticket_IDs_version','Zendesk_Ticket_Count','Component','Incident_Reported_Time','Incident_Resolution_Time','Test_Plan','Test_Plan_content','Test_Plan_type','Test_Plan_version','Review_Comments','Review_Comments_content','Review_Comments_type','Review_Comments_version','Request_participants','Satisfaction','Satisfaction_date','Environment','Affected_Environment','Found_in_Version','Found_in_Version_id','Found_in_Version_self','Found_in_Version_value','Flag','Count','Verification_Phase','Verification_Phase_id','Verification_Phase_self','Verification_Phase_value','Verified_Clouds','Offer_Acceptance_Date','Email','Phone','Manager','Date','Process_Type','Name','Contract_Type','Position','Department/Team','Location','Access_To','Access_To_id','Access_To_self','Access_To_value','Development_Phase','Automated','Automated_id','Automated_self','Automated_value','Effort_Spent','Effort_Spent_id','Effort_Spent_self','Effort_Spent_value','Production_Environments','Effort_Classification','Effort_Classification_id','Effort_Classification_self','Effort_Classification_value','Ops_Review_Required','Ops_Review_Required_id','Ops_Review_Required_self','Ops_Review_Required_value','Security_Impact','Security_Impact_id','Security_Impact_self','Security_Impact_value','productboard_URL','Start_date','Affected_Technologies','Goal','Persona','Audience_Size','customer_affected_count','Story_point_estimate', 'Availability','Availability_id','Availability_self','Availability_value','Launch_stage','Launch_stage_id','Launch_stage_self','Launch_stage_value','Feature_Setting','Feature_Setting_id','Feature_Setting_self','Feature_Setting_value','Feature_flag_name','Internal_release_note','Internal_release_note_content','Internal_release_note_type','Internal_release_note_version','Issue_color','Reviewer','Reviewer_accountId','Reviewer_accountType','Reviewer_active','Reviewer_avatarUrls_16x16','Reviewer_avatarUrls_24x24','Reviewer_avatarUrls_32x32','Reviewer_avatarUrls_48x48','Reviewer_displayName','Reviewer_emailAddress','Reviewer_key','Reviewer_name','Reviewer_self','Reviewer_timeZone','DOC_JIRA','Rollout_start_date','S1','S2','S3','S4','S5','Global','Rollback_date','Project_Manager','Technical_Lead','Product_Manager','Description','description_content','description_type','description_version','Due_date','Environment','environment_content','environment_type','environment_version','Fix_Version/s','Linked_Issues','issuetype_avatarId','issuetype_description','issuetype_iconUrl','issuetype_id','issuetype_name','issuetype_self','issuetype_subtask','jira_id','Labels','Last_Viewed','parent_fields_issuetype_avatarId','parent_fields_issuetype_description','parent_fields_issuetype_iconUrl','parent_fields_issuetype_id','parent_fields_issuetype_name','parent_fields_issuetype_self','parent_fields_issuetype_subtask','parent_fields_priority_iconUrl','parent_fields_priority_id','parent_fields_priority_name','parent_fields_priority_self','parent_fields_status_description','parent_fields_status_iconUrl','parent_fields_status_id','parent_fields_status_name','parent_fields_status_self','parent_fields_status_statusCategory_colorName','parent_fields_status_statusCategory_id','parent_fields_status_statusCategory_key','parent_fields_status_statusCategory_name','parent_fields_status_statusCategory_self','parent_fields_summary','parent_id','parent_key','parent_self','priority_iconUrl','priority_id','priority_name','priority_self','progress_percent','progress_progress','progress_total','project_avatarUrls_16x16','project_avatarUrls_24x24','project_avatarUrls_32x32','project_avatarUrls_48x48','project_id','project_key','project_name','project_projectCategory_description','project_projectCategory_id','project_projectCategory_name','project_projectCategory_self','project_projectTypeKey', 'project_self','Reporter','Resolution','resolution_description','resolution_id','resolution_name','resolution_self','Resolved','Security_Level','status_description','status_iconUrl','status_id','status_name','status_self','status_statusCategory_colorName','status_statusCategory_id','status_statusCategory_key','status_statusCategory_name','status_statusCategory_self','Status_Category_Changed','Sub-tasks','Summary','Remaining_Estimate','Original_Estimate','Time_Spent','timetracking_originalEstimate','timetracking_originalEstimateSeconds','timetracking_remainingEstimate','timetracking_remainingEstimateSeconds','timetracking_timeSpent','timetracking_timeSpentSeconds','Updated','Affects_Version/s','votes_hasVoted','votes_self','votes_votes','watches_isWatching','watches_self','watches_watchCount','worklog_maxResults','worklog_startAt','worklog_total','worklog_worklogs','Work_Ratio']

    def collect_jira_field_names(self):
        '''Method to create a custom field ID to name mapping'''
        self.fields_url = 'https://qubole.atlassian.net/rest/api/3/field'
        # requests.get accesses the fields url with the user and access token for authorization
        # the response of this API call contains the list of custom fields in the Jira API
        response = requests.get(self.fields_url, auth = (self.user, self.access_token))
        response = response.json()

        custom_fields = {}

        # Iterate through the list of fields in the response
        # For each field add the field ID as a key in the custom_fields dictionary
        #   and the field name as the corresponding value
        for field in response:
            custom_fields[field['id']] = field['name']
        self.fields = custom_fields
        #print("in collect jira field - printing field names")
        #print(self.fields)

    def add_user_details(self, user_fields, response):
        #'''Method to collect user relatd data'''
        # The variable user_fields contains a list of all user-related fields in the variable response
        # For loop iterates throuh each user field and adds the details of the user to the dataframe self.user_details
        # User details are added to the dataframe only if the user ID is not in the set self.users - to ensure that 
        # there are no duplicate entries for one user
        for field in user_fields:
            try:
                temp = response['fields'][field]
                response['fields'][field] = response['fields'][field]['accountId']
                #print("temp['accountId']", temp['accountId'])
                if temp['accountId'] not in self.users:
                    self.users.add(temp['accountId'])
                    #print("added in users")
                    temp_data = pd_json.json_normalize(temp)
                    #print("now appending in user details dataframe")
                    self.user_details = self.user_details.append(temp_data, ignore_index = True)
            except Exception as e:
                print('User details could not be retrieved due to the error: ' + str(e))

    def collect_jira_data_by_issue(self, issue_id):
        '''Method to collect jira data based on issue ID passed as argument'''
        issue_id = issue_id.strip(' ')
        url = "https://qubole.atlassian.net/rest/api/3/issue/" + str(issue_id)

        # requests.get accesses the issue url with the issue ID and uses the user and access token
        # for authorization
        response = requests.get(url, auth = (self.user, self.access_token))
        response = response.json()

        # user_fields variable lists the fields which contain user IDs in the response 
        user_fields = ['assignee', 'reporter', 'creator']
        # The list of user fields is passed as argument to the add_user_details function
        # This function adds data to the user_details dataframe
        self.add_user_details(user_fields, response)

        # The variable temp stores the list of comments in the response variable
        temp = response['fields']['comment']['comments']
        # For loop iterates over the comments and stores data for each comment in the self.jira_comments dataframe
        for comment in temp:
            try:
                # Each comment has an author and the details for the author are added to the self.user_details dataframe
                # If the user is not already found in the set self.users
                if comment['author']['accountId'] not in self.users:
                    self.users.add(comment['author']['accountId'])
                    temp_data = pd_json.json_normalize(comment['author'])
                    self.user_details = self.user_details.append(temp_data, ignore_index = True)

                # Replace author details in each comment with the author account ID
                comment['author'] = comment['author']['accountId']

                # Add the Jira issue ID as an additional field in a comment
                # This issue ID acts as a foreign key for the tables that are created from this data
                comment['jira_id'] = response['key']
                comment = pd_json.json_normalize(comment)

                # Append the comment to the dataframe self.jira_comments
                self.jira_comments = self.jira_comments.append(comment, ignore_index = True)

            except Exception as e:
                print('Comment details could not be retrieved due to the error: ' + str(e))

        # Remove all data related to comments from the response
        response['fields'].pop('comment', None)

        # Each attachment also has an author
        # The details for the author are added to the dataframe self.user_details if the user is not already
        # found in the set self.users 
        for attachment in response['fields']['attachment']:
            try:
                if attachment['author']['accountId'] not in self.users:
                    self.users.add(attachment['author']['accountId'])
                    temp_data = pd_json.json_normalize(comment['author'])
                    self.user_details = self.user_details.append(temp_data, ignore_index = True)

                attachment['author'] = attachment['author']['accountId']

            except Exception as e:
                print('Jira details could not be retrieved due to the error: ' + str(e))

        # Add the issue ID in response['key'] to response['fields']['key']
        # as only the response['fields'] section of the json will be stored in
        # the dataframe self.jira_details
        try:
            print("response['key']", response['key'])
            response['fields']['key'] = response['key']
            response = pd_json.json_normalize(response['fields'])
            print("now appending response key in jira details")
            #print("JIRA DATA", response)

            self.jira_details = self.jira_details.append(response, ignore_index = True)
            #print("WHAT IS THE ISSUE MAN", self.jira_details['key'])

        except Exception as e:
            print('User details could not be retrieved due to the error: ' + str(e))

    def create_csv(self):
        '''Method to make final changes and updates to the dataframes and write them to csv files'''

        self.collect_jira_field_names()
        self.user_details.reset_index()
        self.jira_details.reset_index()
        self.jira_comments.reset_index()

    
        new_column_names = []
        # For loop to iterate through the current column names for 
        # the dataframe self.jira_details and replace all custom fields names with 
        # the actual names retrieved using the method collect_jira_field_names earlier

        for column in self.jira_details.columns.values:
            #print("columns in self.jira_details.columns.values", column)
            if column in self.fields.keys():
                new_column_names.append(self.fields[column])
            elif column.count('_') >= 2:
                temp = column.split('_')
                last_val = '_'.join(temp[2:])
                if temp[0] == 'customfield':
                    temp = '_'.join(temp[:2])
                    if temp in self.fields.keys():
                        temp = self.fields[temp] + ' ' + last_val
                        new_column_names.append(temp)
                    else:
                        new_column_names.append(column)
                else:
                    new_column_names.append(column)
            else:
                new_column_names.append(column)

        self.jira_details.columns = new_column_names
        print("IN new_column_names", self.jira_details['key'])

        pd_list = [self.jira_details, self.user_details, self.jira_comments]
        for df in pd_list:
            for col in list(df.columns.values):
                df[col].replace(r'\s+|\\n', ' ', regex=True, inplace=True)
                df[col].replace(r',', '\\,', regex=True, inplace=True)
            #print(df.columns);
            df.columns = df.columns.str.replace(".","_")
            df.columns = df.columns.str.replace(" ","_")
            #print(df.columns);

        # Iterate through the rows in jira_details dataframe and remove all extra 
        # unicode characters added to the text 
        for i, row in self.jira_details.iterrows():
            #print("ROWS:", row)
            #print("printing lables",row.Labels)
            #print("printing customers affected as per the JIRA",row.Customers_Affected)
            temp = str(row.Labels)
            if temp == '[]':
                temp = ''
            else:
                temp = temp[1:-1].replace("u'", "").replace("'", "")
            self.jira_details.loc[i, 'Labels'] = temp

            try:
                linked_tickets = row['Zendesk_Ticket_IDs_content']
                #print('Linked tickets found: ', linked_tickets);
                linked_tickets = linked_tickets[0]['content'][0]['text']
                linked_tickets = linked_tickets.replace("u'", "").replace("'", "")
            except:
                linked_tickets = ''
            self.jira_details.loc[i, 'Zendesk_Ticket_IDs_content'] = linked_tickets
            

            self.jira_details.loc[i, 'Customers_Affected'] = str(row.Customers_Affected).replace('[', '').replace(']','').replace("u'", "").replace("'","")

        #columns_to_be_selected_jira_details_columns = ['Assignee', 'Created', 'Creator', 'Customers_Affected', 'Epic_Link', 'Workaround/_Resolution', 'Release_Notes', 'QA_Engineer', 'doc_impact', 'Team_title', 'Affected_Clouds', 'Organizations', 'Affects_Version/s', 'Environments','Approver','Zendesk_Ticket_IDs','Zendesk_Ticket_Count','Incident_Reported_Time','Incident_Resolution_Time','Verified_Clouds','Development_Phase','Effort_Spent_value','Security_Impact_value','customer_affected_count','Internal_release_note_content','issuetype_name','jira_id','Labels','project_id','project_key','project_name','Resolved','status_name','status_statusCategory_name','Summary','votes_votes','watches_watchcount']
        columns_to_be_selected_jira_details_columns = ['Assignee', 'Created', 'Creator', 'Customers_Affected', 'Zendesk_Ticket_Count', 'issuetype_name', 'key', 'Labels', 'project_id', 'project_key', 'project_name', 'Resolved', 'status_name', 'status_statusCategory_name', 'Summary']
        self.jira_details = self.jira_details.loc[:,columns_to_be_selected_jira_details_columns]

        columns_to_be_selected_jira_user_columns = ['jira_user_id','active','displayName','emailAddress','user_id','name','self', 'timeZone']
        self.user_details = self.user_details.loc[:,columns_to_be_selected_jira_user_columns]

        columns_to_be_selected_jira_comments_columns = ['author', 'body_content', 'created', 'comment_id', 'jira_id', 'updateAuthor_displayName', 'updateAuthor_key', 'updateAuthor_name', 'updateAuthor_timeZone', 'updated']
        self.jira_comments = self.jira_comments.loc[:,columns_to_be_selected_jira_comments_columns]

        print("BEFORE RENAME", self.jira_details['key'])

        # Rename column names to make them consistent
        self.jira_comments = self.jira_comments.rename(columns = {'id':'comment_id'})
        self.jira_comments = self.jira_comments.rename(columns = {'updateAuthor_accountId':'updateAuthor_jira_user_id'})
        self.jira_details = self.jira_details.rename(columns = {'key':'jira_id'})
        self.jira_details = self.jira_details.rename(columns = {'Zendesk_Ticket_IDs_content':'ticket_ids'})
        self.user_details = self.user_details.rename(columns = {'key':'user_id'})
        self.user_details = self.user_details.rename(columns = {'accountId':'jira_user_id'})
        self.user_details = self.user_details.rename(columns = {'status_statusCategory_name':'status_Category_name'})

        print("AFTER RENAME", self.jira_details['jira_id'])

        # DROP ADDITIONAL COLUMNS IN JIRA DETAILS
        # changing this to select columns as necessary: 


        # columns_to_be_dropped = []
        # for column in self.jira_details.columns:
        #     if column not in self.jira_details_columns:
        #         columns_to_be_dropped.append(column)


        #self.jira_details.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'ignore')

        # if len(self.jira_details.columns) < len(self.jira_details_columns):
        #     diff = np.setdiff1d(self.jira_details.columns,self.jira_details_columns, assume_unique=True)
        #     for col in diff:
        #         self.jira_details[col] = ''



        # # DROP ADDITIONAL COLUMNS IN JIRA USERS
        # columns_to_be_dropped = []
        # for column in self.user_details.columns:
        #     if column not in self.jira_user_columns:
        #         columns_to_be_dropped.append(column)


        #self.user_details.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'ignore')

        # if len(self.user_details.columns) < len(self.jira_user_columns):
        #     diff = np.setdiff1d(self.user_details.columns,self.jira_user_columns, assume_unique=True)
        #     for col in diff:
        #         self.user_details[col] = ''




        # # DROP ADDITIONAL COLUMNS IN JIRA COMMENTS
        # columns_to_be_dropped = []
        # for column in self.jira_comments.columns:
        #     if column not in self.jira_comments_columns:
        #         columns_to_be_dropped.append(column)


        #self.jira_comments.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'ignore')

        # if len(self.jira_comments.columns) < len(self.jira_comments_columns):
        #     diff = np.setdiff1d(self.jira_comments.columns,self.jira_comments_columns, assume_unique=True)
        #     for col in diff:
        #         self.jira_comments[col] = ''

        self.jira_comments.columns = [x.lower() for x in self.jira_comments.columns]
        self.jira_details.columns = [x.lower() for x in self.jira_details.columns]

        print("LAST", self.jira_details['jira_id'])
        self.user_details.columns = [x.lower() for x in self.user_details.columns]

        # Create a list of all dataframes
        jira_pds = [self.user_details, self.jira_details, self.jira_comments]
        # Iterate through the list of dataframes and replace all new lines characters by a space character
        # Replace all commas with '\\,' to ensure proper table creation using OpenCSV serde later in the 
        # data pipeline
        for df in jira_pds:
            for col in list(df.columns.values):
                df[col].replace(r'\s+|\\n', ' ', regex=True, inplace=True)
                df[col].replace('\n', '', inplace=True)
                df[col].replace(r',', '\\,', regex=True, inplace=True)
            #print(df.columns)
            df.columns = df.columns.str.replace(".","_")

        # For loop to modify the body_content column in the dataframe self.jira_comments
        # The body_content column contains embedded json fields and this loop removes content 
        # from the embedded json fields and creates a string
        for i, row in self.jira_comments.iterrows():
            text = ''
            body = self.jira_comments.at[i, 'body_content']
            body = body[0]['content']
            #body = body[0].content
            for sentence in body:
                if 'text' in sentence.keys():
                    text += (sentence['text'] + '\n')
                elif 'mention' in sentence.keys():
                    text += (sentence['attrs']['text'])
            self.jira_comments.at[i, 'body_content'] = text

        # Write the dataframe to csv files
        print("writing the JIRA iles to csv format");
        self.user_details.to_csv('jira_user_details.csv', encoding = 'utf-8', index = False)
        self.jira_details.to_csv('jira_details.csv', encoding = 'utf-8', index = False)
        self.jira_comments.to_csv('jira_comments.csv', encoding = 'utf-8', index = False)
        print("finished writing the JIRA iles to csv format");

class ZendeskDataCollection:
    '''Class to collect zendesk data for all tickets created after January 1st, 2018'''
    def __init__(self, credentials, domain, bounding_date, jira_access_token, jira_user, creation_bounding_date):
        """Constructor function for the class ZendeskDataCollection"""
        # In the arguments, the bounding date represents the update bounding date 
        # All tickets created on or after the created_bounding_date and updated on or after the (update) bounding_date 
        # will be collected and stored

        # Credentials is a tuple with the zendesk user and API token details 
        # using which the API will be accessed
        #print("IN ZendeskDataCollection class, Constructor");
        self.credentials = credentials
        self.zendesk = domain
        self.params = {
                        'query': ('type:ticket created>' + creation_bounding_date),
                        'sort_by': 'updated_at',
                        'sort_order': 'desc'
                    }



        self.bounding_date = bounding_date
        #print("self.params:", self.params)

        # Variable definition for all urls that will be used to access the data
        self.tickets_url = self.zendesk + '/api/v2/search.json?' + urlencode(self.params)
        #print("self.tickets_url:",self.tickets_url)
        self.users_url = self.zendesk + '/api/v2/users/'
        self.organizations_url = self.zendesk + '/api/v2/organizations/'
        self.comments_url = self.zendesk + '/api/v2/tickets/' # {id}/comments.json?include=users'
        self.fields_url = self.zendesk + '/api/v2/ticket_fields.json'

        # Dataframes creation to store data related to tickets, users, organizations and comments
        
        self.ticket_details = pd.DataFrame()
        self.users = set()
        self.user_details = pd.DataFrame()
        self.organizations = set()
        self.organization_details = pd.DataFrame()
        self.comments = pd.DataFrame()
        self.ticket_jira_mapping = pd.DataFrame()

        self.jira_access_token = jira_access_token
        self.jira_user = jira_user
        self.ticket_jira_mapping_jira_list = []
        self.ticket_jira_mapping_zendesk_list = []

        # Object creation for the class JiraDataCollection
        self.jira_data_collector = JiraDataCollection(jira_access_token, jira_user)

        self.zd_organization_columns = ['organization_created_at', 'organization_details', 'organization_domain_names', 'organization_external_id', 'organization_group_id', 'organization_id', 'organization_name', 'organization_notes',  'organization_organization_fields_account_escalation', 'organization_organization_fields_account_type', 'organization_organization_fields_contract_renewal_date', 'organization_organization_fields_cs_service_level', 'organization_organization_fields_csm1', 'organization_organization_fields_edition', 'organization_organization_fields_gainsight_stage', 'organization_organization_fields_org_cc', 'organization_organization_fields_solutions_architect', 'organization_organization_fields_support_notes', 'organization_organization_fields_support_plan', 'organization_shared_comments', 'organization_shared_tickets', 'organization_tags', 'organization_updated_at', 'organization_url']
        self.zd_user_columns = ['user_active', 'user_alias', 'user_chat_only', 'user_created_at', 'user_custom_role_id', 'user_default_group_id', 'user_details', 'user_email', 'user_external_id', 'user_iana_time_zone', 'user_id', 'user_last_login_at', 'user_locale', 'user_locale_id', 'user_moderator', 'user_name', 'user_notes', 'user_only_private_comments', 'user_organization_id', 'user_permanently_deleted', 'user_phone user_photo', 'user_photo_content_type', 'user_photo_content_url', 'user_photo_file_name', 'user_photo_height', 'user_photo_id', 'user_photo_inline', 'user_photo_mapped_content_url', 'user_photo_size', 'user_photo_thumbnails', 'user_photo_url', 'user_photo_width', 'user_report_csv', 'user_restricted_agent', 'user_role', 'user_role_type', 'user_shared', 'user_shared_agent', 'user_shared_phone_number', 'user_signature', 'user_suspended', 'user_tags', 'user_ticket_restriction', 'user_time_zone', 'user_two_factor_auth_enabled', 'user_updated_at', 'user_url', 'user_user_fields_agent_ooo', 'user_user_fields_jira_ids', 'user_user_fields_user_cc', 'user_user_fields_user_notes', 'user_verified']
        self.zd_tickets_columns = ['Additional Solution Given', 'Closure Code', 'Cluster ID', 'Documentation Needed?', 'Issue Summary', 'Issue Type', 'Job ID', 'jira_ids', 'Permission to troubleshoot', 'Qubole Account Name', 'Qubole Email ID', 'Qubole Environment', 'Recent changes to customer Qubole environment', 'Severity - Business Impact', 'Support Plan', 'Time spent last update (sec)', 'Total time spent (sec)', 'Urgent Ticket Explanation', 'What can we help you with today?', 'allow_attachments', 'allow_channelback', 'assignee_id', 'brand_id', 'collaborator_ids', 'created_at', 'description', 'due_at', 'email_cc_ids', 'external_id', 'follower_ids', 'followup_ids', 'forum_topic_id', 'group_id', 'has_incidents', 'ticket_id', 'is_public', 'organization_id', 'priority', 'problem_id', 'raw_subject', 'recipient', 'requester_id', 'result_type', 'satisfaction_probability', 'sharing_agreement_ids', 'status', 'subject', 'submitter_id', 'tags', 'ticket_form_id', 'type', 'updated_at', 'url', 'satisfaction_score', 'satisfaction_comment', 'via_source_name', 'via_source_address', 'via_channel']
        self.zd_comments_columns = ['attachments', 'audit_id', 'author_id', 'body', 'created_at', 'comment_id', 'metadata', 'plain_body', 'public', 'ticket_id', 'type', 'via']

    def add_user_details(self, user_ids):
        '''Method to add user details for user IDs passed in the argument to the
           dataframe self.user_details'''
        #print("in add_user_details")   
        for user in user_ids:
            if user not in self.users:
                self.users.add(user)
                response = session.get(self.users_url + str(user) + '.json')
                if response.status_code == 200:
                    user_info = response.json()
                    user_info = pd_json.json_normalize(user_info)
                    # print(user_info['user.organization_id'])
                    org_id = user_info['user.organization_id'].values
                    # print(org_id) 
                    self.add_organization_details(org_id[0])
                    self.user_details = self.user_details.append(user_info, ignore_index = True)

    def add_organization_details(self, organization):
        '''Method to add organization details for organization IDs passed in the argument
           to the dataframe self.organization_details'''
        #print("in add_organization_details")  
        if organization not in self.organizations:
            self.organizations.add(organization)
            response = session.get(self.organizations_url + str(organization) + '.json')
            if response.status_code == 200:
                organization_info = response.json()
                organization_info = pd_json.json_normalize(organization_info)
                self.organization_details = self.organization_details.append(organization_info, ignore_index = True)

    def collect_data(self):
        '''Method to collect tickets data and modify it as needed'''
        #print("In collect_data")
        self.fields = {}
        # While loop to collect data for Zendesk fields and store it in the dataframe self.fields
        while(self.fields_url):
            response = session.get(self.fields_url)
            if response.status_code != 200:
                print('Error with status code {}'.format(response.status_code))
                print('Could not retrieve Zendesk field details. Exiting ...')
                exit()

            data = response.json()
            #print(data)
            #print("This is the response object from zendesk",data['count'])

            #print("previous page" , data["previous_page"])
            #print("next page", data["next_page"])

            #data is a json object with attributes: 1) a list ticket_fields, count, previous_page, next_page
            #for field in data:
                #print("printing field response data as retreived from Zendesk API:", data)

              # While loop to collect data for Zendesk fields and store it in the dataframe self.fields
            for field in data['ticket_fields']:
                #print("field title", field['title'])
                #print("field id", field['id'])
                self.fields[field['id']] = field['title']
            self.fields_url = data['next_page']


        flag = 0

        while self.tickets_url:
            #print("self.tickets_url: ",self.tickets_url)
            if flag == 1:
                break
            response = session.get(self.tickets_url)
            if response.status_code != 200:
                print('Error with status code {}'.format(response.status_code))
                exit()
            data = response.json()
            count = 0

            # Iterate through the tickets in the results field of the json
            # Choose all tickets with a update bounding date greater than the update bounding date 
            # passed as an argument at the time of object creation
            #print("printing data again- ", data)
            for ticket in data['results']:
                update_date = datetime.strptime(ticket['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                print("update_date", update_date)
                bounding_date = datetime.strptime(self.bounding_date, '%Y-%m-%dT%H:%M:%SZ')
                print("bounding_date", bounding_date)
                #print(update_date)
                #print(bounding_date)

                if update_date < bounding_date:
                    #print("COUNT:",data['results'][:count])
                    data['results'] = data['results'][:count]
                    #print("RESULTING DATA WHEN VALIDATING DATES", data['results'])
                    print("update_date < bounding_date")
                    flag = 1
                    print("breaking out now")
                    break
                count += 1

                associated_jiras = ''
                #print("ticket['custom_fields']", ticket['custom_fields'])
                custom_fields = ticket['custom_fields']

                #print("ticket custom_fields", custom_fields)
                #rint("self.fields.keys()", self.fields.keys())

                for field in custom_fields:
                    if field['id'] in self.fields.keys():
                        ticket[self.fields[field['id']]] = field['value']
                    if str(field['id']) == '25356346':
                        associated_jiras = field['value']
                        mapping_jiras = field['value']

                        print("associated_jiras", associated_jiras)
                        print("ticket_id", ticket['id'])

                        if mapping_jiras != None:
                            mapping_jiras = mapping_jiras.split(',')
                            for jira in mapping_jiras:
                                # add a logic to check if JIRA_id exists; if yes, first drop that entry and then upload the new one
                                self.ticket_jira_mapping_jira_list.append(jira)
                                self.ticket_jira_mapping_zendesk_list.append(ticket['id'])

                        if mapping_jiras == None: 
                            self.ticket_jira_mapping_jira_list.append("N/A")
                            self.ticket_jira_mapping_zendesk_list.append(ticket['id'])


                        # for jira in associated_jiras:
                        #     self.ticket_jira_mapping_jira_list.append(jira)
                        #     self.ticket_jira_mapping_zendesk_list.append(ticket['id'])
                        #     print("jira_id_list", self.ticket_jira_mapping_jira_list)
                        #     print("ticket_id_list", self.ticket_jira_mapping_zendesk_list)

                #print(self.ticket_jira_mapping_jira_list)
                #print(self.ticket_jira_mapping_zendesk_list)

                # ticket.pop('custom_fields', None)

                # print associated_jiras
                if associated_jiras != None:
                    associated_jiras = associated_jiras.split(',')
                    for jira in associated_jiras:
                        #self.ticket_jira_mapping_jira_list.append(jira)
                        #self.ticket_jira_mapping_zendesk_list.append(ticket['id'])
                        
                        if jira.find('-') != -1:
                            if jira.find('http') == -1:
                                try:
                                    self.jira_data_collector.collect_jira_data_by_issue(jira)
                                except Exception as e:
                                    print('Jira details for ' + str(jira) + ' could not be retrieved due to the error: ' + str(e)) 

                #print(associated_jiras)

                
 
                user_ids = [ticket['assignee_id']] + ticket['collaborator_ids'] + ticket['follower_ids'] + [ticket['submitter_id']] + [ticket['requester_id']]
                user_ids = list(set(user_ids))

                self.add_user_details(user_ids)

                organization = ticket['organization_id']
                self.add_organization_details(organization)

                ticket_id = ticket['id']
                #print(ticket_id)
                ticket_comments_url = self.comments_url + str(ticket_id) + '/comments.json?include=users'
                while(ticket_comments_url):
                    try:
                        response = session.get(ticket_comments_url)
                        if response.status_code == 200:
                            ticket_comments = response.json()
                            ticket_comments_url = ticket_comments['next_page']
                            ticket_comments = pd_json.json_normalize(ticket_comments, record_path = 'comments')
                            ticket_comments['ticket_id'] = ticket_id
                            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                            #   print(ticket_comments.head())

                            user_ids = list(ticket_comments['author_id'].values)
                            self.add_user_details(user_ids)

                            ticket_comments = ticket_comments.drop('html_body', axis = 1)
                            self.comments = self.comments.append(ticket_comments, ignore_index = True)

                    except Exception as e:
                        print('Ticket comments could not be retrieved due to: ' + str(e))

                
            #print("ONE LAST TIME", data)
            ticket_data = pd_json.json_normalize(data, record_path = 'results')
            #print("PRINTING TICKET DATA",ticket_data)
            self.ticket_details = self.ticket_details.append(ticket_data, ignore_index = True)
            #print("SELF TICKET DETAILS",self.ticket_details)
           # print("TICKET DATA", ticket_data)

            self.tickets_url = data['next_page']



        # cols to be selected (['Closure Code','Issue Type', 'Qubole Environment', 'Recent changes to customer Qubole environment', 'Time spent last update (sec)', 'Total time spent (sec)', 'assignee_id', 'brand_id', 'collaborator_ids', 'created_at', 'description', 'follower_ids', 'group_id', 'ticket_id', 'organization_id', 'priority', 'recipient', 'requester_id', 'status', 'subject', 'submitter_id', 'updated_at', 'satisfaction_score', 'satisfaction_comment'])
        # DROP ADDITIONAL COLUMNS IN TICKETS
        # columns_to_be_dropped = []
        # for column in self.ticket_details.columns:
        #     if column not in self.zd_tickets_columns:
        #         columns_to_be_dropped.append(column)

       # self.ticket_details.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'true')

        # if len(self.ticket_details.columns) < len(self.zd_tickets_columns):
        #     diff = np.setdiff1d(self.ticket_details.columns,self.zd_tickets_columns, assume_unique=True)
        #     for col in diff:
        #         self.ticket_details[col] = ''

        # DROP ADDITIONAL COLUMNS IN USERS
        # columns_to_be_dropped = []
        # for column in self.user_details.columns:
        #     if column not in self.zd_user_columns:
        #         columns_to_be_dropped.append(column)

        # #self.user_details.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'true')

        # if len(self.user_details.columns) < len(self.zd_user_columns):
        #     diff = np.setdiff1d(self.user_details.columns,self.zd_user_columns, assume_unique=True)
        #     for col in diff:
        #         self.user_details[col] = ''

        # # DROP ADDITIONAL COLUMNS IN ORGANIZATIONS
        # columns_to_be_dropped = []
        # for column in self.organization_details.columns:
        #     if column not in self.zd_organization_columns:
        #         columns_to_be_dropped.append(column)

        #self.organization_details.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'ignore')

        # if len(self.organization_details.columns) < len(self.zd_organization_columns):
        #     diff = np.setdiff1d(self.organization_details.columns,self.zd_organization_columns, assume_unique=True)
        #     for col in diff:
        #         self.organizations_details[col] = ''

        # DROP ADDITIONAL COLUMNS IN TICKET COMMENTS
       #  columns_to_be_dropped = []
       #  for column in self.comments.columns:
       #      if column not in self.zd_comments_columns:
       #          columns_to_be_dropped.append(column)

       # # self.comments.drop(columns_to_be_dropped, axis = 1, inplace = True, errors = 'ignore')

       #  if len(self.comments.columns) < len(self.zd_comments_columns):
       #      diff = np.setdiff1d(self.comments.columns,self.zd_comments_columns, assume_unique=True)
       #      for col in diff:
       #          self.comments[col] = ''



        # self.comments['body'].replace(r'\s+|\\n', ' ', regex=True, inplace=True)
        # self.comments['plain_body'].replace(r'\s+|\\n', ' ', regex=True, inplace=True)

        pd_list = [self.comments, self.ticket_details, self.user_details, self.organization_details]
        for df in pd_list:
            for col in list(df.columns.values):
                #print(df.columns.values)
                #print(df[col])
                df[col].replace(r'\s+|\\n', ' ', regex=True, inplace=True)
                df[col].replace(r',', '\\,', regex=True, inplace=True)
            #print(df.columns);
            df.columns = df.columns.str.replace(".", "_")
            #print("printing ticket details before failure",self.ticket_details);
            #print("type of ticket_details ", type(self.ticket_details))
            #print("printing columns in df:", self.ticket_details.columns)
            #print("custom field",custom_fields)
            #print(ticket_details['custom_fields'])
        # try:
        #     self.ticket_details.drop(['custom_fields'], axis = 1, inplace = True, errors = True)
        
        # except Exception as e:
        #     print("Could not drop the \"custom fields\" column", + str(e))

        # try:
        #     self.ticket_details.drop(['fields'], axis=1, inplace= True,  errors = True)
        
        # except Exception as e:
        #     print("Could not drop the \"fields\" column again!!!!!", str(e))

        # satisfaction_score = []
        # satisfaction_comment = []
        # for row in self.ticket_details['satisfaction_rating']:
        #   if row['score'] == 'unoffered':
        #       satisfaction_score.append('unoffered')
        #       satisfaction_comment.append('')
        #   else:
        #       satisfaction_score.append(row['score'])
        #       satisfaction_comment.append(row['comment'])

        # self.ticket_details.insert(45, 'satisfaction_score', satisfaction_score)
        # self.ticket_details.insert(46, 'satisfaction_comment', satisfaction_comment)

        self.ticket_details['satisfaction_score'] = ''
        self.ticket_details['satisfaction_comment'] = ''

        self.ticket_details['via_source_name'] = ''
        self.ticket_details['via_source_address'] = ''
        self.ticket_details['via_channel'] = ''

        for i, row in self.ticket_details.iterrows():
            self.ticket_details.at[i, 'satisfaction_score'] = row['satisfaction_rating']['score']
            if row['satisfaction_rating']['score'] != 'unoffered':
                self.ticket_details.at[i, 'satisfaction_comment'] = row['satisfaction_rating']['comment']

            self.ticket_details.at[i, 'via_channel'] = row['via']['channel']
            if row['via']['channel'] == 'email':
                self.ticket_details.at[i, 'via_source_name'] = row['via']['source']['from']['name']
                self.ticket_details.at[i, 'via_source_address'] = row['via']['source']['from']['address']

            self.ticket_details.loc[i, 'tags'] = str(row['tags']).replace('[', '').replace(']','').replace("u'", "").replace("'","")
            self.ticket_details.loc[i, 'follower_ids'] = str(row['follower_ids']).replace('[', '').replace(']','').replace("u'", "").replace("'","")
            self.ticket_details.loc[i, 'collaborator_ids'] = str(row['collaborator_ids']).replace('[', '').replace(']','').replace("u'", "").replace("'","")
            self.ticket_details.loc[i, 'Documentation Needed?'] = str(row['Documentation Needed?']).replace('[', '').replace(']','').replace("u'", "").replace("'","")
            self.ticket_details.loc[i, 'Additional Solution Given'] = str(row['Additional Solution Given']).replace('[', '').replace(']','').replace("u'", "").replace("'","")

        #self.ticket_details.drop(['satisfaction_rating', 'via'], axis = 1, inplace = True, errors = 'true')
        

        for i, row in self.organization_details.iterrows():
            #print("printing organization name and rows")
            #print("i is:", i)
            #print("row is", row)
            #print(type(row))
            #print(row[2])
            #temp = str(row['organization_domain_names'])
            temp = str(row.organization_domain_names)
            if temp == '[]':
                temp = ''
            else:
                temp = temp[1:-1].replace("u'", "").replace("'", "")
            self.organization_details.at[i, 'organization_domain_names'] = temp
            self.organization_details.loc[i, 'organization_tags'] = str(row.organization_domain_names).replace('[', '').replace(']','').replace("u'", "").replace("'","")

        #self.comments.drop(['formatted_from', 'formatted_to', 'data', 'transcription_visible', 'trusted'], axis = 1, inplace = True, errors = 'true')

        self.comments = self.comments.rename(columns = {'id':'comment_id'})
        self.ticket_details = self.ticket_details.rename(columns = {'id':'ticket_id'})
        self.ticket_details = self.ticket_details.rename(columns = {'Linked JIRA IDs':'jira_ids'})

        columns_to_be_selected_zd_tickets_columns = ['Closure Code','Issue Type', 'Qubole Environment', 'Recent changes to customer Qubole environment', 'Time spent last update (sec)', 'Total time spent (sec)', 'assignee_id', 'brand_id', 'collaborator_ids', 'created_at', 'description', 'follower_ids', 'group_id', 'ticket_id', 'organization_id', 'priority', 'recipient', 'requester_id', 'status', 'subject', 'submitter_id', 'updated_at', 'satisfaction_score', 'satisfaction_comment']
        self.ticket_details = self.ticket_details[columns_to_be_selected_zd_tickets_columns]
        #print("ticket_details cols", self.ticket_details.columns)

        columns_to_be_selected_zd_user_columns = ['user_active', 'user_created_at', 'user_custom_role_id', 'user_default_group_id', 'user_email', 'user_id', 'user_name', 'user_organization_id', 'user_permanently_deleted','user_role', 'user_time_zone', 'user_updated_at', 'user_url']
        self.user_details = self.user_details.loc[:,columns_to_be_selected_zd_user_columns]
        #print("user_details cols",self.user_details.columns )

        columns_to_be_selected_zd_organization_columns = ['organization_created_at', 'organization_domain_names', 'organization_group_id', 'organization_id', 'organization_name',  'organization_organization_fields_account_escalation', 'organization_organization_fields_account_type', 'organization_organization_fields_contract_renewal_date', 'organization_organization_fields_cs_service_level', 'organization_organization_fields_csm1', 'organization_organization_fields_edition', 'organization_organization_fields_gainsight_stage', 'organization_organization_fields_org_cc', 'organization_organization_fields_solutions_architect', 'organization_tags', 'organization_updated_at', 'organization_url']
        self.organization_details = self.organization_details.loc[:,columns_to_be_selected_zd_organization_columns]
        #print("organization_details cols",self.organization_details.columns)

        columns_to_be_selected_zd_comments_columns = ['author_id', 'created_at', 'comment_id', 'plain_body', 'public', 'ticket_id']
        self.comments = self.comments.loc[:,columns_to_be_selected_zd_comments_columns]
        #print("comments cols", self.comments.columns)


        self.ticket_jira_mapping = pd.DataFrame({'jira_id':self.ticket_jira_mapping_jira_list,'ticket_id':self.ticket_jira_mapping_zendesk_list})
        self.ticket_jira_mapping.to_csv("ticket_jira_mapping.csv", encoding = 'utf-8', index = False)

        self.ticket_details.columns = [x.lower() for x in self.ticket_details.columns]
        self.user_details.columns = [x.lower() for x in self.user_details.columns]
        self.organization_details.columns = [x.lower() for x in self.organization_details.columns]
        self.comments.columns = [x.lower() for x in self.comments.columns]


        self.ticket_details.to_csv("ticketdetails.csv", encoding = 'utf-8', index = False)
        self.user_details.to_csv("userdetails.csv", encoding = 'utf-8', index = False)
        self.organization_details.to_csv("organizationdetails.csv", encoding = 'utf-8', index = False)
        self.comments.to_csv("ticketcomments.csv", encoding = 'utf-8', index=False)

        self.jira_data_collector.create_csv()

if __name__ == "__main__":
    credentials = 'slack-bot@qubole.com/token' , sys.argv[3]

    #bounding_date = '2019-10-14'
    bounding_date = sys.argv[1]
    bounding_date += 'T00:00:00Z'

    jira_access_token = sys.argv[2]
    jira_user = 'slack-bot@qubole.com'

    # bounding_date = '2019-05-14'
    # bounding_date += 'T12:00:00Z'
    session = requests.Session()
    session.auth = credentials

    #jira_data_collector = JiraDataCollection(jira_access_token, jira_user)
    #jira_data_collector.collect_jira_field_names()
    #jira_data_collector.collect_jira_data_by_issue('ACM-2706')
    #jira_data_collector.create_csv()

    zendesk = 'https://qubole.zendesk.com'
    creation_bounding_date = '2018-12-31'
    #print("Data is being retrieved from date:", creation_bounding_date)

    zendesk_data_collector = ZendeskDataCollection(credentials, zendesk, bounding_date, jira_access_token, jira_user, creation_bounding_date)
    #print("bounding_date is: ", bounding_date,"This is the date that is compared with the update date of the ticket and thus it is retrieved ")
    #print("both jira and zendesk class constructors have been initialized")
    #print("now collect data function is being called")
    zendesk_data_collector.collect_data()

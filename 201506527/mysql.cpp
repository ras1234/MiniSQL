#include<bits/stdc++.h>
#include <unistd.h>
#include <fstream>
using namespace std;

int QueryColumnDistinct_status = 0,QueryDistinct_status=0;
int fault_status = 0;
int querydrop_status=0;
int querytruncate_status=0;
int querydelete_status=0;
int querycreate_status=0;

map<string,vector<string> > database_metadatadefinition;
map<string,vector<vector<int> > > Database_RecordsStored;
vector<string>Input_SQL_Query;
vector<vector<string> > SelectAggregateWhereColumns;
vector<string> Constraints_Select;
vector<string> CrossProductOfTables;
vector<string> CrossProductTablesDefinition;
vector<vector<int> >RecordsDataofJoinTables;
vector<vector<int> >ResultantRecords;
vector<pair<string,vector<int> > > Sql_query_resultstored;
vector<pair<string,vector<float> > > FloatingpointRecordstored;

void databaseStoredataFunc();
void InputQueryParsedFunc(string query);
void ReadInputqueryfunc();
void CrossProducFunc();
void Combined_TablesFunc();
int FindColumnsDataFunc(string s);
void FindSelectColdatafunc();
void FindSelectedDatafunc();
void MYsqlPrompt();
void databaseDeffunc();
vector<int> select_rows(int Case);
string EraseFindFunc(string inp);
bool MatchingOfTwoStrings( const std::string& str1, const std::string& str2 ) ;
bool CaseNumberForJoin(const std::string& s);

void MYsqlPrompt()                              //mysql prompt 
{ 
    char *cwdname=NULL;
    cwdname=getcwd(cwdname, 2000);
    printf("MySql@%s$",cwdname);
} 
 

int main(int argc,char *argv[])
{
    while(1)
    {   
        database_metadatadefinition.clear();
        Database_RecordsStored.clear();
        Input_SQL_Query.clear();
        SelectAggregateWhereColumns.clear();
        Constraints_Select.clear();
        CrossProductOfTables.clear();
        CrossProductTablesDefinition.clear();
        RecordsDataofJoinTables.clear();
        ResultantRecords.clear();
        Sql_query_resultstored.clear();
        FloatingpointRecordstored.clear();
    
        QueryColumnDistinct_status = 0,QueryDistinct_status=0;
        fault_status = 0;
        querydrop_status=0;
        querytruncate_status=0;
        querydelete_status=0;
        querycreate_status=0; 
        
        MYsqlPrompt(); 
        string argvname="";
        getline(cin, argvname);
        char *sqlquery=(char *)malloc(sizeof(char)*(1000));
        strcpy(sqlquery,argvname.c_str());

        if(sqlquery[strlen(sqlquery)-1]==';')
        {
            sqlquery[strlen(sqlquery)-1]='\0';
        }

        databaseDeffunc(); //return metadata definitions
        if(fault_status==1)
            continue;
        databaseStoredataFunc();//returns database tables records
        if(fault_status==1)
            continue;
        InputQueryParsedFunc(sqlquery);//returns Input_SQL_Query
        if(querycreate_status==1)
        {
            querycreate_status=0;
            continue;
        }
        if(fault_status==1)
            continue;
        ReadInputqueryfunc();//returns aggregate conditions
        if(querytruncate_status==1)
        {
            querytruncate_status=0;
            continue;
        }
        if(querydelete_status==1)
        {   
            querydelete_status=0;
            continue;
        }
        if(querydrop_status==1)
        {
            querydrop_status=0;
            continue;
        }
        if(fault_status==1)
            continue;
        Combined_TablesFunc();//returns CrossProductTablesDefinition 
        if(fault_status==1)
            continue;
        FindSelectedDatafunc();
        if(fault_status==1)
            continue;
    } 
        return 0; 
}

string EraseFindFunc(string SqlQuery)
{
    SqlQuery.erase(SqlQuery.begin(), std::find_if(SqlQuery.begin(), SqlQuery.end(), std::bind1st(std::not_equal_to<char>(), ' ')));
    SqlQuery.erase(std::find_if(SqlQuery.rbegin(), SqlQuery.rend(), std::bind1st(std::not_equal_to<char>(), ' ')).base(), SqlQuery.end());
    return SqlQuery;
}

bool MatchingOfTwoStrings( const std::string& FirstString, const std::string& SecondString ) 
{
    std::string FirstStringCopy( FirstString );
    std::string SecondStringCopy( SecondString );
    std::transform( FirstStringCopy.begin(), FirstStringCopy.end(), FirstStringCopy.begin(), ::tolower );
    std::transform( SecondStringCopy.begin(), SecondStringCopy.end(), SecondStringCopy.begin(), ::tolower );
    return ( FirstStringCopy == SecondStringCopy );
}


bool CaseNumberForJoin(const std::string& strcasenum)
{
    std::string::const_iterator it = strcasenum.begin();
    if(strcasenum.find("-")!=std::string::npos)
    {
        it++;
    }
    while (it != strcasenum.end() && (std::isdigit(*it)))
    {
        ++it;
    }
    return !strcasenum.empty() && it == strcasenum.end();
}

void databaseDeffunc()
{
    ifstream nameOfMetafile ("metadata.txt");
    if(nameOfMetafile.is_open())
    {
        string table_name;
        int metadatafile_state = 0; //0--closed 1--begin 2---table is opened
        string metafileLineno;
        while(nameOfMetafile >> metafileLineno)
        {
            if(metafileLineno=="<begin_table>")
            {
                metadatafile_state = 1;

            }
            else if(metafileLineno=="<end_table>")
                metadatafile_state = 0;
            else if(metadatafile_state==1)
            {
                vector<string> vec_a;
                database_metadatadefinition[metafileLineno] = vec_a;
                table_name = metafileLineno;
                metadatafile_state = 2;
            }
            else if(metadatafile_state==2)
            {
                database_metadatadefinition[table_name].push_back(metafileLineno);
            }
        }
    }
    /*for(map<string, vector<string> >::const_iterator myiterator=database_metadatadefinition.begin();myiterator!=database_metadatadefinition.end();++myiterator)
    {
        cout << myiterator->first << endl;
        for(int i=0;i<myiterator->second.size();i++)
            cout << myiterator->second[i] << " ";
        cout << endl;
    }*/
    nameOfMetafile.close();
}



void databaseStoredataFunc()
{
    for(map<string, vector<string> >::const_iterator mydataiterator=database_metadatadefinition.begin();mydataiterator!=database_metadatadefinition.end();++mydataiterator)
    {
        //cout << mydataiterator->first << endl;
        string gettablename = mydataiterator->first;
        const char *s2 = ".csv";
        char *s1 = new char[gettablename.length()+1];
        strcpy(s1,gettablename.c_str());               //to convert char * to const char *
        ifstream data_file (strcat(s1,s2));        //to create file name gettablename.csv
        string dataline;
        while(data_file >> dataline)
        {
            vector<int> rowvector;
            stringstream ss(dataline);
            string tokenname;
            char delim = ',';
            while(getline(ss,tokenname,delim))
            {
                tokenname.erase(remove( tokenname.begin(), tokenname.end(), '\"' ),tokenname.end());
            rowvector.push_back(atoi(tokenname.c_str()));
            }
            Database_RecordsStored[gettablename].push_back(rowvector);
        }
    }
   /* for(map<string, vector<vector<int> > >::const_iterator mydataiterator=Database_RecordsStored.begin(); mydataiterator!=Database_RecordsStored.end(); ++mydataiterator)
    {
        cout << mydataiterator->first << endl;
        for(int i=0;i<mydataiterator->second.size();i++)
        {
            for(int j=0;j<mydataiterator->second[i].size();j++)
            {
                cout << mydataiterator->second[i][j] << " ";
            }
            cout << endl;
        }
    }  */
}

void InputQueryParsedFunc(string givenquery)
{
    cout << givenquery << endl;
    char deliminator = ' ';
    stringstream obj(givenquery);
    string tokenname;
    int create_pos=-1;
    if((givenquery.find("CREATE")!=std::string::npos) || (givenquery.find("create")!=std::string::npos) || (givenquery.find("Create")!=std::string::npos) )
    {
        create_pos=0;
    }
    while(getline(obj,tokenname,deliminator))
    {   
        if(create_pos!=-1)
        {
            if (tokenname.find(',')!= std::string::npos)
            {   
                stringstream streamssobject1(tokenname);
                string tokenname2;
                char delimname=',';
                while (getline(streamssobject1, tokenname2, delimname))
                {
                    Input_SQL_Query.push_back(tokenname2);
                }
            }
            else if (tokenname.find('(')!= std::string::npos)
            {   
                stringstream streamssobject2(tokenname);
                string tokenname3;
                char delimname='(';
                while (getline(streamssobject2, tokenname3, delimname))
                {
                    Input_SQL_Query.push_back(tokenname3);
                }
            }
            else if (tokenname.find(')')!= std::string::npos)
            {   
                stringstream streamssobject3(tokenname);
                string tokenname4;
                char delimname=')';
                while (getline(streamssobject3, tokenname4, delimname))
                {
                    Input_SQL_Query.push_back(tokenname4);
                }
            }
            else
            {
                Input_SQL_Query.push_back(tokenname);
            }
        }
        else
        {
            if (tokenname.find(',')!= std::string::npos)
            {   
                stringstream streamssobject1(tokenname);
                string tokenname2;
                char delimname=',';
                while (getline(streamssobject1, tokenname2, delimname))
                {
                    Input_SQL_Query.push_back(tokenname2);
                }
            }
            else
            {
                Input_SQL_Query.push_back(tokenname);
            }
        }
        
    }
  
   /* cout<<Input_SQL_Query.size()<<endl;
    for(int i=0;i<Input_SQL_Query.size();i++)
    {
        cout<<Input_SQL_Query[i]<<" ";
    }
    cout<<endl; */

    create_pos=-1;
    int drop_pos=-1;
    int truncate_pos=-1;
    int delete_pos=-1;
    for(int i=0;i<Input_SQL_Query.size();i++)
    {
         if((MatchingOfTwoStrings(Input_SQL_Query[i],"CREATE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"create")))
        {
            create_pos = i;
            continue;
        }
        if((MatchingOfTwoStrings(Input_SQL_Query[i],"DROP")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"drop")))
        {
            drop_pos = i;
            continue;
        }
        if((MatchingOfTwoStrings(Input_SQL_Query[i],"TRUNCATE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"truncate")))
        {
            truncate_pos = i;
            continue;
        }
        if((MatchingOfTwoStrings(Input_SQL_Query[i],"DELETE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"delete")))
        {
            delete_pos = i;
            continue;
        }

    }
    if(delete_pos!=-1)
    {
        vector<string> delete_vec;
        string tempstring1;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            if((MatchingOfTwoStrings(Input_SQL_Query[i],"DELETE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"delete")))
            {
                delete_pos = i;
                querydelete_status=1;
                continue;
            }
            if(delete_pos!=-1)
            {
                if((Input_SQL_Query[i].find("where")!=std::string::npos)||(Input_SQL_Query[i].find("WHERE")!=std::string::npos)||(Input_SQL_Query[i].find("FROM")!=std::string::npos) || (Input_SQL_Query[i].find("from")!=std::string::npos) )
                {
                    if(!tempstring1.empty())
                    {
                        //cout << "tempstring1 " << tempstring1 << endl;
                        delete_vec.push_back(tempstring1);
                    }
                    delete_vec.push_back(Input_SQL_Query[i]);
                    tempstring1.clear();
                }
                else
                {
                    tempstring1 += Input_SQL_Query[i];
                }
            }
        }
       
        if(!tempstring1.empty())
            delete_vec.push_back(tempstring1);

        int vectsize = Input_SQL_Query.size();
        for(int i=delete_pos+1;i<vectsize;i++)
        {
            Input_SQL_Query.erase(Input_SQL_Query.begin() + i);
        }
        for(int i=0;i<delete_vec.size();i++)
        {
            Input_SQL_Query.push_back(delete_vec[i]);
        }
  
       /*cout << Input_SQL_Query.size() << endl;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            cout << Input_SQL_Query[i] << " ";
        }
        cout << endl; */
    }
    if(truncate_pos!=-1)
    {
        vector<string> truncate_vec;
        string tempstring1;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            if((MatchingOfTwoStrings(Input_SQL_Query[i],"TRUNCATE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"truncate")))
            {
                truncate_pos = i;
                querytruncate_status=1;
                continue;
            }
            if(truncate_pos!=-1)
            {
                if((Input_SQL_Query[i].find("TABLE")!=std::string::npos) || (Input_SQL_Query[i].find("table")!=std::string::npos) )
                {
                    if(!tempstring1.empty())
                    {
                        //cout << "tempstring1 " << tempstring1 << endl;
                        truncate_vec.push_back(tempstring1);
                    }
                    truncate_vec.push_back(Input_SQL_Query[i]);
                    tempstring1.clear();
                }
                else
                {
                    tempstring1 += Input_SQL_Query[i];
                }
            }
        }
       
        if(!tempstring1.empty())
            truncate_vec.push_back(tempstring1);

        int vectsize = Input_SQL_Query.size();
        for(int i=truncate_pos+1;i<vectsize;i++)
        {
            Input_SQL_Query.erase(Input_SQL_Query.begin() + i);
        }
        for(int i=0;i<truncate_vec.size();i++)
        {
            Input_SQL_Query.push_back(truncate_vec[i]);
        }
  
      /* cout << Input_SQL_Query.size() << endl;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            cout << Input_SQL_Query[i] << " ";
        }
        cout << endl; */
    }
    if(drop_pos!=-1)
    {
        vector<string> drop_vec;
        string tempstring1;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            if((MatchingOfTwoStrings(Input_SQL_Query[i],"DROP")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"drop")))
            {
                drop_pos = i;
                querydrop_status=1;
                continue;
            }
            if(drop_pos!=-1)
            {
                if((Input_SQL_Query[i].find("TABLE")!=std::string::npos) || (Input_SQL_Query[i].find("table")!=std::string::npos) )
                {
                    if(!tempstring1.empty())
                    {
                        //cout << "tempstring1 " << tempstring1 << endl;
                        drop_vec.push_back(tempstring1);
                    }
                    drop_vec.push_back(Input_SQL_Query[i]);
                    tempstring1.clear();
                }
                else
                {
                    tempstring1 += Input_SQL_Query[i];
                }
            }
        }
       
        if(!tempstring1.empty())
        drop_vec.push_back(tempstring1);

        int vectsize = Input_SQL_Query.size();
        for(int i=drop_pos+1;i<vectsize;i++)
        {
            Input_SQL_Query.erase(Input_SQL_Query.begin() + i);
        }
        for(int i=0;i<drop_vec.size();i++)
        {
            Input_SQL_Query.push_back(drop_vec[i]);
        }
  
     /*   cout << Input_SQL_Query.size() << endl;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            cout << Input_SQL_Query[i] << " ";
        }
        cout << endl; */
    }
    if(create_pos!=-1)
    {
        vector<string> create_vec;
        string tempstring1;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            if((MatchingOfTwoStrings(Input_SQL_Query[i],"CREATE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"create")))
            {
                create_pos = i;
                querycreate_status=1;
                continue;
            }
            if(create_pos!=-1)
            {
                if((Input_SQL_Query[i].find("FLOAT")!=std::string::npos)||(Input_SQL_Query[i].find("float")!=std::string::npos)||(Input_SQL_Query[i].find("table")!=std::string::npos) || (Input_SQL_Query[i].find("TABLE")!=std::string::npos) || (Input_SQL_Query[i].find("INT")!=std::string::npos) || (Input_SQL_Query[i].find("int")!=std::string::npos))
                {
                    if(!tempstring1.empty())
                    {
                        //cout << "tempstring1 " << tempstring1 << endl;
                        create_vec.push_back(tempstring1);
                    }
                    create_vec.push_back(Input_SQL_Query[i]);
                    tempstring1.clear();
                }
                else
                {
                    tempstring1 += Input_SQL_Query[i];
                }
            }
        }
       
        if(!tempstring1.empty())
        create_vec.push_back(tempstring1);

        int vectsize = Input_SQL_Query.size();
        for(int i=create_pos+1;i<vectsize;i++)
        {
            Input_SQL_Query.erase(Input_SQL_Query.begin() + i);
        }
        for(int i=0;i<create_vec.size();i++)
        {
            Input_SQL_Query.push_back(create_vec[i]);
        }
  
     /*   cout << Input_SQL_Query.size() << endl;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            cout << Input_SQL_Query[i] << " ";
        }
        cout << endl; */

        string create_tablename=Input_SQL_Query[2];
        vector<string> create_rows;
        for(int i=3;i<Input_SQL_Query.size();i++)
        {
            if(i%2==1)
            {
                create_rows.push_back(Input_SQL_Query[i]);
            }
        }
        for(int i=4;i<Input_SQL_Query.size();i++)
        {
            if(i%2==0)
            {
                if((Input_SQL_Query[i].find("float")!=std::string::npos) || (Input_SQL_Query[i].find("FLOAT")!=std::string::npos))
                {
                    cout<<"values should be float type"<<endl;
                    return;
                }    
            }
        }
      /*  cout<<create_rows.size()<<endl;
        for(int i=0;i<create_rows.size();i++)
            cout<<create_rows[i]<<endl;  */
        ifstream createfile("metadata.txt");
        string createline;
        ofstream newcreatefile;
        newcreatefile.open("new_create.txt");
        while(createfile >> createline)
        {   
            if(createline==create_tablename)
            {
                cout<<"filename already exist"<<endl;
                fault_status=1;
                return ;
            }
            newcreatefile << createline;
            newcreatefile << "\n";
        }
        newcreatefile << "<begin_table>";
        newcreatefile << "\n";
        newcreatefile << create_tablename;
        newcreatefile << "\n";
        for(int i=0;i<create_rows.size();i++)
        {
                newcreatefile << create_rows[i];
                newcreatefile << "\n";
        }
        newcreatefile << "<end_table>";
        newcreatefile << "\n";
        createfile.close();
        newcreatefile.close();
        ifstream f;
        char oldname[] ="new_create.txt";
        char newname[] ="metadata.txt";
        f.open("new_create.txt");
        rename( oldname , newname );
        f.close(); 
        string cr_newtable;
        cr_newtable=Input_SQL_Query[2];
        cr_newtable=cr_newtable+"."+"csv";
        string str=cr_newtable;
        ofstream cr_file;
        cr_file.open(str);
        cr_file.close();
        ifstream f4;
        char coldname[] ="new_mfile.txt";
        char cnewname[] ="metadata.txt";
        f.open("new_mfile.txt");
        rename( coldname , cnewname );
        f.close(); 
        return;
    }

    int where_pos = -1;
    string tempstring1;
    vector<string>where_vec;
    for(int i=0;i<Input_SQL_Query.size();i++)
    {
        if((MatchingOfTwoStrings(Input_SQL_Query[i],"WHERE")) || (MatchingOfTwoStrings(Input_SQL_Query[i],"where")))
        {
            where_pos = i;
            continue;
        }
        if(where_pos!=-1)
        {
            if((Input_SQL_Query[i].find("and")!=std::string::npos) || (Input_SQL_Query[i].find("AND")!=std::string::npos) || (Input_SQL_Query[i].find("or")!=std::string::npos) || (Input_SQL_Query[i].find("OR")!=std::string::npos))
            {
                if(!tempstring1.empty())
                {
                    //cout << "tempstring1 " << tempstring1 << endl;
                    where_vec.push_back(tempstring1);
                }
                where_vec.push_back(Input_SQL_Query[i]);
                tempstring1.clear();
            }
            else
            {
                tempstring1 += Input_SQL_Query[i];
            }
        }
    }
    if(where_pos==-1)
    {
        return;
    }
    if(!tempstring1.empty())
        where_vec.push_back(tempstring1);

    int vectsize = Input_SQL_Query.size();
    for(int i=where_pos+1;i<vectsize;i++)
    {
        Input_SQL_Query.erase(Input_SQL_Query.begin() + i);
    }
    for(int i=0;i<where_vec.size();i++)
    {
        Input_SQL_Query.push_back(where_vec[i]);
    }
 
  /*  cout << Input_SQL_Query.size() << endl;
    for(int i=0;i<Input_SQL_Query.size();i++)
    {
        cout << Input_SQL_Query[i] << " ";
    }
    cout << endl; */
}


void ReadInputqueryfunc()
{
    int select_pos = -1, from_pos = -1, where_pos = -1;
    int drop_pos=-1,table_pos=-1,delete_pos=-1;
    int truncate_pos=-1;
    for(int i=0;i<Input_SQL_Query.size();i++)
    {   
        if(MatchingOfTwoStrings(Input_SQL_Query[i],"DELETE"))
        {
            delete_pos=i;
            querydelete_status=1;
        }
        if(MatchingOfTwoStrings(Input_SQL_Query[i],"TRUNCATE"))
        {
            truncate_pos=i;
            querytruncate_status=1;
        }

        if(MatchingOfTwoStrings(Input_SQL_Query[i],"DROP"))
        {
            drop_pos=i;
            querydrop_status=1;
        }
        if(MatchingOfTwoStrings(Input_SQL_Query[i],"TABLE") || MatchingOfTwoStrings(Input_SQL_Query[i],"table"))
        {
            table_pos=i;
        }
        if(MatchingOfTwoStrings(Input_SQL_Query[i],"SELECT"))
        {
            select_pos=i;
        }
        else if(MatchingOfTwoStrings(Input_SQL_Query[i],"FROM"))
        {
            from_pos=i;
        }
        else if(MatchingOfTwoStrings(Input_SQL_Query[i],"WHERE"))
        {
            where_pos=i;
        }
    }

    if(querydrop_status==1)
    {
        int droptabstatus=0;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            if(Input_SQL_Query[i]=="TABLE" || Input_SQL_Query[i]=="table")
                droptabstatus=1;
        }
        if(droptabstatus==0)
        {
            cout<<"Error"<<endl;
            return ;
        }
        if(droptabstatus==1)
        {
            int droptablestatus1=0;
            for(map<string, vector<string> >::const_iterator it=database_metadatadefinition.begin();it!=database_metadatadefinition.end();++it)
            {
                if(it->first==Input_SQL_Query[Input_SQL_Query.size()-1])   
                    droptablestatus1=1;   
            }
            if(droptablestatus1!=1)
            {
                cout<<"Table doesnt exist"<<endl;
                return ;
            }
        }

        string truncate_tablename=Input_SQL_Query[Input_SQL_Query.size()-1];
        string truncate_tablename_withextension=truncate_tablename+"."+"csv";
        ifstream inputFile(truncate_tablename_withextension);
        if(inputFile.peek() == std::ifstream::traits_type::eof())
        {
             std::ofstream ofs;
        ofs.open (truncate_tablename_withextension, std::ofstream::out | std::ofstream::app & std::ofstream::out | std::ofstream::trunc);
        ofs.close();

        string drop_tablename=truncate_tablename;
        vector<string> drop_temp;
        ifstream oldfile("metadata.txt");
        string line;
        while(oldfile >>line)
        {
            drop_temp.push_back(line);
        }
        oldfile.close();
        int start_index,end_index;
        int i;
        for( i=0;i<drop_temp.size();i++)
        {
            if(drop_temp[i]==drop_tablename)
            {
                start_index=i-1;
                break;
            }

        }
        for(int j=i;i<drop_temp.size();j++)
        {
            if(drop_temp[j]=="<end_table>")
            {
                end_index=j;
                break;
            }
        }
        //cout<<start_index<<" "<<end_index<<endl;
        vector<string> drop_temp1;
        for(int k=0;k<drop_temp.size();k++)
        {
            if(k<start_index || k>end_index)
            {
                drop_temp1.push_back(drop_temp[k]);
            }
            
        }
        /*for(int k=0;k<truncate_temp1.size();k++)
            cout<<truncate_temp1[k]<<" ";
        cout<<endl;  */

        ofstream newfile;
        newfile.open ("new_metadata.txt");
        for(int k=0;k<drop_temp1.size();k++)
        {
             newfile << drop_temp1[k];
             newfile << "\n";
        }
        newfile.close();
        oldfile.close();
        ifstream f;
        char oldname[] ="new_metadata.txt";
        char newname[] ="metadata.txt";
        f.open("metadata.txt");
        rename( oldname , newname );
        truncate_tablename=truncate_tablename+"."+"csv";
        string strdelete=truncate_tablename;
        remove(strdelete.c_str());
        f.close();
        inputFile.close();
        return ;
        }
        else
        {
            cout<<"File is not Empty"<<endl;
            return ;
        }
    }

    if(querytruncate_status==1)
    {
        int truncatetabstatus=0;
        for(int i=0;i<Input_SQL_Query.size();i++)
        {
            if(Input_SQL_Query[i]=="TABLE" || Input_SQL_Query[i]=="table")
                truncatetabstatus=1;
        }
        if(truncatetabstatus==0)
        {
            cout<<"Error"<<endl;
            return ;
        }
        if(truncatetabstatus==1)
        {
            int truncatetablestatus1=0;
            for(map<string, vector<string> >::const_iterator it=database_metadatadefinition.begin();it!=database_metadatadefinition.end();++it)
            {
                if(it->first==Input_SQL_Query[Input_SQL_Query.size()-1])   
                    truncatetablestatus1=1;   
            }
            if(truncatetablestatus1!=1)
            {
                cout<<"Table doesnt exist"<<endl;
                return ;
            }
        }
        string truncate_tablename=Input_SQL_Query[Input_SQL_Query.size()-1];
        string drop_tablename=Input_SQL_Query[Input_SQL_Query.size()-1];
        string truncate_tablename_withextension=truncate_tablename+"."+"csv";
        std::ofstream ofs;
        ofs.open (truncate_tablename_withextension, std::ofstream::out | std::ofstream::app & std::ofstream::out | std::ofstream::trunc);
        ofs.close();
        return ;
    } 
    if(querydelete_status==1)
    {
        for(int i=0;i<Input_SQL_Query.size();i++)
        {   
            if(MatchingOfTwoStrings(Input_SQL_Query[i],"FROM") || (MatchingOfTwoStrings(Input_SQL_Query[i],"from")))
            {
                from_pos=i;
            }
            if(MatchingOfTwoStrings(Input_SQL_Query[i],"WHERE") || (MatchingOfTwoStrings(Input_SQL_Query[i],"where")))
            {
                where_pos=i;
            }

        }
        if(from_pos==-1 && where_pos==-1)
        {
            cout<<"Error: from and where clause is missing"<<endl;
            return ;
        }
        if(from_pos==-1)
        {
            cout<<"Error: From clause is missing"<<endl;
            return ;
        }
        if(where_pos==-1)
        {
            cout<<"Error : Where clause is missing"<<endl;
            return;
        }
        int deletetablestatus1=0;
        for(map<string, vector<string> >::const_iterator it=database_metadatadefinition.begin();it!=database_metadatadefinition.end();++it)
        {
            if(it->first==Input_SQL_Query[from_pos+1])   
                deletetablestatus1=1;   
        }
        if(deletetablestatus1!=1)
        {
            cout<<"Table doesnt exist"<<endl;
            return ;
        }

        //cout<<Input_SQL_Query[from_pos+1]<<endl;
        //cout<<Input_SQL_Query[where_pos+1]<<endl;
        string delete_colname,delete_tablename_withattribute;
        string delete_colvalue;
        if(Input_SQL_Query[where_pos+1].find("=")!=std::string::npos)
        {
            delete_colvalue =Input_SQL_Query[where_pos+1].substr(Input_SQL_Query[where_pos+1].find("=")+1,Input_SQL_Query[where_pos].size()-Input_SQL_Query[where_pos+1].find("."));
            delete_tablename_withattribute= Input_SQL_Query[where_pos+1].substr(0,Input_SQL_Query[where_pos+1].find("="));
        }
        string delete_tablename;
        if(delete_tablename_withattribute.find(".")!=std::string::npos)
        {
          delete_tablename = delete_tablename_withattribute.substr(0,delete_tablename_withattribute.find("."));  
          delete_colname = delete_tablename_withattribute.substr(delete_tablename_withattribute.find(".")+1,delete_tablename_withattribute.size());  
        }
        else
        {
            delete_colname=delete_tablename_withattribute;
        }
        //delete_tablename=Input_SQL_Query[from_pos+1];
        //cout<<delete_tablename<<endl;
       // cout<<delete_colname<<endl;
       // cout<<delete_colvalue<<endl;
        if(delete_tablename=="")
        {
            delete_tablename=Input_SQL_Query[from_pos+1];
        }
        if(Input_SQL_Query[from_pos+1]!=delete_tablename)
        {
            cout<<"Error"<<endl;
            return ;
        }
       
        ifstream del_metafile("metadata.txt");
        string line;
        int tablename_match=0;
        int line_index=0;
        int delete_columnindex;
        while(del_metafile >> line)
        {
            if(tablename_match==1)
            {
                if(line=="<end_table>")
                {
                    cout<<"Column doesnt exist"<<endl;
                    return ;
               }
                else if(line==delete_colname)
                {
                    delete_columnindex=line_index;
                    break;
                }
                else if(line!="end_table")
                {
                    line_index++;
                }

            }
            else 
            {
                if(line=="<begin_table>")
                {
                    del_metafile >>line;
                    if(line==delete_tablename)
                    {
                        tablename_match=1;
                    }
                }
            }
        }
        del_metafile.close();
        vector<int> delete_rowsindex;
        for(map<string, vector<vector<int> > >::const_iterator it=Database_RecordsStored.begin(); it!=Database_RecordsStored.end(); ++it)
        {   
            //cout<<it->first<<endl;
            if(it->first == delete_tablename)
            {
                for(int i=0;i<it->second.size();i++)
                {  
                    for(int j=0;j<it->second[i].size();j++)
                   {
                        if(j==delete_columnindex)
                        {
                            if(it->second[i][j]==stoi(delete_colvalue))
                                delete_rowsindex.push_back(i);
                        }
                    }
                }
            }  
        }  
        /*for(int i=0;i<delete_rowsindex.size();i++)
        {
            cout<<delete_rowsindex[i]<<" ";
        }
        cout<<endl;  */
        const char *b = ".csv";
        char *a = new char[delete_tablename.length()+1];
        strcpy(a,delete_tablename.c_str());               //to convert char * to const char *
        ifstream deletedata_file(strcat(a,b));        //to create file name temptablename.csv
        string deletedatafile_line;
        ofstream after_deletefile;
        int deletefileindex=0;
        int deleterow_status=0;
        after_deletefile.open ("newtable.csv");
        vector<string> newfiledata;
        while(deletedata_file >>line)
        {
            for(int i=0;i<delete_rowsindex.size();i++)
            {
                if(deletefileindex!=delete_rowsindex[i])
                    deleterow_status=1;
                if(deleterow_status==1)
                {
                    newfiledata.push_back(line);
                    deleterow_status=0;
                }
            }
            deletefileindex++;
        }
       /* for(int i=0;i<newfiledata.size();i++)
            cout<<newfiledata[i]<<" ";
        cout<<endl;  */
        for(int i=0;i<newfiledata.size();i++)
        {
            line=newfiledata[i];
            after_deletefile << line;
            after_deletefile << "\n";
        } 
        deletedata_file.close();
        after_deletefile.close();

        ifstream f;
        char oldname[] ="newtable.csv";
        char newname[]="";
        for(int i=0;i<delete_tablename.size();i++)
                newname[i]=delete_tablename[i];
        string newname1(newname);
        newname1=newname1+"."+"csv";
        char newname2[]="";
        for(int i=0;i<newname1.size();i++)
            newname2[i]=newname1[i];
        newname2[newname1.size()]='\0';
        f.open("newtable.csv");
        rename( oldname , newname2 );
        f.close();
        return ;
    }

    vector<string> temp;
    if(select_pos==-1)
    {
        cout << "Error" << endl;
        fault_status = 1;
        return;
    }
    else
    {
        int distinctconstStatus = 0,tempFlag = 0;
        for(int i=select_pos+1;i<from_pos;i++)
        {
            if(Input_SQL_Query[i]=="")
            {
            }
            else
            {
                vector<string> PropertiesOfColumns;
                Input_SQL_Query[i] = EraseFindFunc(Input_SQL_Query[i]);
                if(distinctconstStatus==1)
                {
                    PropertiesOfColumns.push_back("DISTINCT");
                    distinctconstStatus = 0;
                }
                else if(Input_SQL_Query[i]=="*")
                {
                    vector<string> PropertiesOfColumns;
                    PropertiesOfColumns.push_back("NONE");    // max, avg, min 
                    PropertiesOfColumns.push_back("NONE");    // table
                    PropertiesOfColumns.push_back("ALL");     // column
                    SelectAggregateWhereColumns.push_back(PropertiesOfColumns);
                }
                else if(Input_SQL_Query[i].find("max")!=std::string::npos || Input_SQL_Query[i].find("MAX")!=std::string::npos)
                {
                    tempFlag = 1;

                    PropertiesOfColumns.push_back("MAX");
                }
                else if(Input_SQL_Query[i].find("min")!=std::string::npos || Input_SQL_Query[i].find("MIN")!=std::string::npos)
                {
                    tempFlag = 1;
                    PropertiesOfColumns.push_back("MIN");
                }
                else if(Input_SQL_Query[i].find("sum")!=std::string::npos || Input_SQL_Query[i].find("SUM")!=std::string::npos)
                {
                    tempFlag = 1;
                    PropertiesOfColumns.push_back("SUM");
                }
                else if(Input_SQL_Query[i].find("distinct")!=std::string::npos || Input_SQL_Query[i].find("DISTINCT")!=std::string::npos)
                {
                    QueryDistinct_status = 1;
                    if((Input_SQL_Query[i].find("(")!=std::string::npos)&(Input_SQL_Query[i].find(")")!=std::string::npos))
                    {
                        QueryColumnDistinct_status = 1;
                        PropertiesOfColumns.push_back("DISTINCT");
                    }
                    else if((Input_SQL_Query[i].find("(")!=std::string::npos)&(Input_SQL_Query[i].find(")")==std::string::npos))
                    {
                        cout << "ERROR" << endl;
                        fault_status = 1;
                        return;
                    }
                    else if((Input_SQL_Query[i].find("(")==std::string::npos)&(Input_SQL_Query[i].find(")")!=std::string::npos))
                    {
                        cout << "ERROR" << endl;
                        fault_status = 1;
                        return;
                    }
                    else
                    {
                        distinctconstStatus = 1;
                    } 
                }
                else if(Input_SQL_Query[i].find("avg")!=std::string::npos || Input_SQL_Query[i].find("AVG")!=std::string::npos)
                {
                    tempFlag = 1;
                    PropertiesOfColumns.push_back("AVG");
                }
                else
                {
                    PropertiesOfColumns.push_back("NONE");
                }
                if(PropertiesOfColumns.size()>0)
                {
                    if(tempFlag==1)
                    {
                        tempFlag = 0;
                        if((Input_SQL_Query[i].find("(")==std::string::npos)&(Input_SQL_Query[i].find(")")==std::string::npos))
                        {
                            cout << "ERROR" << endl;
                            fault_status = 1;
                            return;
                        }
 
                        else if((Input_SQL_Query[i].find("(")!=std::string::npos)&(Input_SQL_Query[i].find(")")==std::string::npos))
                        {
                            cout << "ERROR" << endl;
                            fault_status = 1;
                            return;
                        }
                        else if((Input_SQL_Query[i].find("(")==std::string::npos)&(Input_SQL_Query[i].find(")")!=std::string::npos))
                        {   
                            cout << "ERROR" << endl;
                            fault_status = 1;
                            return;
                        }
                    }
                    string strProduct;
                    if((PropertiesOfColumns[0]!="NONE"))
                    {
                        strProduct=Input_SQL_Query[i].substr(Input_SQL_Query[i].find("(")+1,Input_SQL_Query[i].find(")")-Input_SQL_Query[i].find("(")-1);
                        strProduct=EraseFindFunc(strProduct);
                    }
                    else
                    {
                        strProduct = EraseFindFunc(Input_SQL_Query[i]);
                    }
                    if(strProduct.find(".")!=std::string::npos)
                    {
                        string tempcolumnname = strProduct.substr(strProduct.find(".")+1,strProduct.size()-strProduct.find("."));
                        string temptablename = strProduct.substr(0,strProduct.find("."));
                        PropertiesOfColumns.push_back(temptablename);
                        PropertiesOfColumns.push_back(tempcolumnname);
                    }
                    else
                    {
                        PropertiesOfColumns.push_back("NONE");
                    
                        PropertiesOfColumns.push_back(strProduct);
                    }
                    SelectAggregateWhereColumns.push_back(PropertiesOfColumns);
                }
                PropertiesOfColumns.clear();
            }
        }
    }

    /*for(int i=0;i<SelectAggregateWhereColumns.size();i++)
    {
                for(int j=0;j<SelectAggregateWhereColumns[i].size();j++)
                {
                        cout<<SelectAggregateWhereColumns[i][j]<<" ";
                }
                printf("\n");
    }*/
    if(from_pos==-1)
    {
        printf("Error: No table to select from\n");
        fault_status = 1;
        return;
    }
    else
    {
        int locationEnd=0;
        if(where_pos==-1)
        {
            locationEnd=Input_SQL_Query.size();
        }
        else
        {
            locationEnd = where_pos;
        }
        for(int i=from_pos+1;i<locationEnd;i++)
        {
            if(Input_SQL_Query[i]=="")
            {
            }
            else
            {
                Input_SQL_Query[i] = EraseFindFunc(Input_SQL_Query[i]);
                CrossProductOfTables.push_back(Input_SQL_Query[i]);
            }
        }
    }
    /*for(int i=0;i<CrossProductOfTables.size();i++)
    {
        cout << CrossProductOfTables[i] << " ";
    }
    cout << endl;*/
    if(where_pos==-1)
    {
        //printf("No conditions\n");
    }
    else
    {
        //printf("Conditions to select\n");
        for(int i=where_pos+1;i<Input_SQL_Query.size();i++)
        {
            string temps1,temps2,temps3,temps4,temps5,temps6;
            if(Input_SQL_Query[i]=="")
            {
            }
            else
            {
                Input_SQL_Query[i] = EraseFindFunc(Input_SQL_Query[i]);
                if(Input_SQL_Query[i].find("AND")!=std::string::npos || Input_SQL_Query[i].find("and")!=std::string::npos)
                {
                    Constraints_Select.push_back("AND");
                }
                else if(Input_SQL_Query[i].find("OR")!=std::string::npos || Input_SQL_Query[i].find("or")!=std::string::npos)
                {
                    Constraints_Select.push_back("OR");
                }
                else
                {
                    string condition;
                    if(Input_SQL_Query[i].find(">=")!=std::string::npos)
                    {
                        temps1=Input_SQL_Query[i].substr(0,Input_SQL_Query[i].find(">="));
                        temps2=Input_SQL_Query[i].substr(Input_SQL_Query[i].find(">=")+2,Input_SQL_Query[i].size()-Input_SQL_Query[i].find(">=")+1);
                        condition = "greater_equal";
                    }
                    else if(Input_SQL_Query[i].find("<=")!=std::string::npos)
                    {
                        temps1=Input_SQL_Query[i].substr(0,Input_SQL_Query[i].find("<="));
                        temps2=Input_SQL_Query[i].substr(Input_SQL_Query[i].find("<=")+2,Input_SQL_Query[i].size()-Input_SQL_Query[i].find("<=")+1);
                        condition = "less_equal";
                    }
                    else if(Input_SQL_Query[i].find("!=")!=std::string::npos || Input_SQL_Query[i].find("<>")!=std::string::npos)
                    {
                        temps1=Input_SQL_Query[i].substr(0,Input_SQL_Query[i].find("!="));
                        temps2=Input_SQL_Query[i].substr(Input_SQL_Query[i].find("!=")+2,Input_SQL_Query[i].size()-Input_SQL_Query[i].find("!=")+1);
                        condition = "not_equal";
                    }
                    else if(Input_SQL_Query[i].find("=")!=std::string::npos)
                    {
                        temps1=Input_SQL_Query[i].substr(0,Input_SQL_Query[i].find("="));
                        temps2=Input_SQL_Query[i].substr(Input_SQL_Query[i].find("=")+1,Input_SQL_Query[i].size()-Input_SQL_Query[i].find("="));
                        condition = "equal";
                    }
                    else if(Input_SQL_Query[i].find(">")!=std::string::npos)
                    {
                        temps1=Input_SQL_Query[i].substr(0,Input_SQL_Query[i].find(">"));
                        temps2=Input_SQL_Query[i].substr(Input_SQL_Query[i].find(">")+1,Input_SQL_Query[i].size()-Input_SQL_Query[i].find(">"));
                        condition = "greater";
                    }
                    else if(Input_SQL_Query[i].find("<")!=std::string::npos)
                    {
                        temps1=Input_SQL_Query[i].substr(0,Input_SQL_Query[i].find("<"));
                        temps2=Input_SQL_Query[i].substr(Input_SQL_Query[i].find("<")+1,Input_SQL_Query[i].size()-Input_SQL_Query[i].find("<"));
                        condition = "lesser";
                    }
                    else
                    {
                        cout << "ERROR" << endl;
                        fault_status = 1;
                        return;
                    }
                    temps1 = EraseFindFunc(temps1);
                    temps2 = EraseFindFunc(temps2);
                    if(temps1.find(".")!=std::string::npos)
                    {
                        temps3=temps1.substr(temps1.find(".")+1,temps1.size()-temps1.find("."));
                        temps4=temps1.substr(0,temps1.find("."));
                        Constraints_Select.push_back(temps4);
                        Constraints_Select.push_back(temps3);
                    }
                    else
                    {
                        Constraints_Select.push_back("NONE");
                        Constraints_Select.push_back(temps1);
                    }
                    Constraints_Select.push_back(condition);
                    if(temps2.find(".")!=std::string::npos)
                    {
                        temps3=temps2.substr(temps2.find(".")+1,temps2.size()-temps2.find("."));
                        temps4=temps2.substr(0,temps2.find("."));
                        Constraints_Select.push_back(temps4);
                        Constraints_Select.push_back(temps3);
                    }
                    else
                    {
                        Constraints_Select.push_back("NONE");
                        Constraints_Select.push_back(temps2);
                    }
                }
            }
        }
    }
    while(Constraints_Select.size()!=11)
    {
        Constraints_Select.push_back("NONE");
    }
    /*for(int i=0;i<Constraints_Select.size();i++)
    {
        cout<<Constraints_Select[i]<<" ";
    }
    cout<<"\n";*/
}

void CrossProducFunc()
{
        for(int i=0;i<CrossProductOfTables.size();i++)
        {
                if(Database_RecordsStored.find(CrossProductOfTables[i])==Database_RecordsStored.end())
                {
                        fault_status=1;
                        printf("error\n");exit(0);
                        return;
                }
        }
        if(CrossProductOfTables.empty())
        {
                fault_status=1;
                printf("error\n");exit(0);
                return;
        }
        else
        {
                map<string,vector<vector<int> > >::iterator tablesdefinitions;
                string table_name = CrossProductOfTables[0];
                tablesdefinitions = Database_RecordsStored.find(table_name);
                vector<vector<int> >table1_data = (*tablesdefinitions).second;
                vector<int> temp3;
                for(int i=1;i<CrossProductOfTables.size();i++)
                {
                        string tablename2=CrossProductOfTables[i];
                        vector<vector<int> > currentDataStored;
                        for(int j=0; j < table1_data.size(); j++)
                        {
                                for(int k=0; k < Database_RecordsStored[tablename2].size(); k++)
                                {
                                        vector<int> currentRowname = table1_data[j];
                                        for(int l=0; l < Database_RecordsStored[tablename2][k].size(); l++)
                                        {
                                                currentRowname.push_back(Database_RecordsStored[tablename2][k][l]);
                                        }
                                        currentDataStored.push_back(currentRowname);
                                        currentRowname.clear();
                                }
                        }
                        table1_data = currentDataStored;
                }
                RecordsDataofJoinTables = table1_data;
             /*   for(int i=0;i<RecordsDataofJoinTables.size();i++)
                {
                        for(int j=0;j<RecordsDataofJoinTables[i].size();j++)
                        {
                                printf("%d ",RecordsDataofJoinTables[i][j]);
                        }
                        printf("\n");
                }
                int s=RecordsDataofJoinTables.size();
                printf("%d\n",s); */
        }
}

void Combined_TablesFunc()
{
    for(int i=0;i<CrossProductOfTables.size();i++)
    {
        string temporarytable = CrossProductOfTables[i];
        temporarytable = EraseFindFunc(temporarytable);
        map<string,vector<string> >::iterator myiterator;
        myiterator=database_metadatadefinition.find(temporarytable);
        if(myiterator==database_metadatadefinition.end()) //check if the table name is valid 
        {
            fault_status=1;
            printf("error\n");
            exit(0);
            return;
        }
        else
        {
            vector<string> table_content=(*myiterator).second;
            for(int j=0;j<table_content.size();j++)
            {
                string tempcolumn = temporarytable + "." + table_content[j];
                CrossProductTablesDefinition.push_back(tempcolumn); //store table.col in metadata
            }
        }
    }
    /*cout << "metadata" << endl;
    for(int i=0;i<CrossProductTablesDefinition.size();i++)
    {
        cout<<CrossProductTablesDefinition[i]<<" ";
    }
    printf("\n");*/
    CrossProducFunc();//returns RecordsDataofJoinTables where cross product of all tables is inserted
}

int FindColumnsDataFunc(string tempstr)
{
    int i=0;
    if(tempstr.find(".")!=std::string::npos)
    {
        int myindex=-1;
        for(i=0;i<CrossProductTablesDefinition.size();i++)
        {
            if(CrossProductTablesDefinition[i]==tempstr)
            {
                myindex=i;
                break;
            }
        }
        if(myindex==-1)
        {
            printf("error\n");
            exit(0);
        }
        else
        {
            return myindex;
        }
    }
    else
    {
        int myindex=-1;
        string str1;
        for(i=0;i<CrossProductTablesDefinition.size();i++)
        {
            str1=CrossProductTablesDefinition[i].substr(CrossProductTablesDefinition[i].find(".")+1,CrossProductTablesDefinition[i].size()-1-CrossProductTablesDefinition[i].find("."));
            if(str1==tempstr)
            {
                if(myindex!=-1)
                {
                    printf("error\n");
                    exit(0);
                }
                myindex=i;
            }                       
        }
        if(myindex==-1)
        {
            printf("error\n");
            exit(0);
        }
        else
        {
            return myindex;
        }
    }
}

vector<int> select_rows(int CaseNumber)
{
    vector<int> result_row;
    int columnIndex1,tableIndex1,columnIndex2,tableIndex2,operatorIndex;
    if(CaseNumber==1)
    {
        columnIndex1 = 1;
        tableIndex1 = 0;
        columnIndex2 = 4;
        tableIndex2 = 3;
        operatorIndex = 2;
    }
    else if(CaseNumber==2)
    {
        columnIndex1 = 7;
        tableIndex1 = 6;
        columnIndex2 = 10;
        tableIndex2 = 9;
        operatorIndex = 8;
    }
    string columnname1=Constraints_Select[columnIndex1];
    //cout<<Constraints_Select[columnIndex1]<<endl;
    string tablename1=Constraints_Select[tableIndex1];
    string final_table1;
    if(CaseNumberForJoin(columnname1))
    {
        printf("error\n");exit(0);
        fault_status = 1;
        return result_row;
    }
    else if(tablename1=="NONE")
    {
        final_table1=columnname1;
    }
    else
    {
        final_table1=tablename1+"."+columnname1;
    }
    int compare1_index = -1;
    compare1_index=FindColumnsDataFunc(final_table1);
    if(compare1_index==-1)
    {
        fault_status=1;
        printf("error\n");exit(0);
        return result_row;
    }
    //cout << "compare1-index " << compare1_index << endl;
    int comparision3=INT_MIN,comparisonOfINdex2=-1;//comparision3 to check if second item is a number and comparisonOfINdex2-column after operator
    string columnname2=Constraints_Select[columnIndex2];
    string tablename2=Constraints_Select[tableIndex2];
    string final_table2;
    int tempColStatus = 1;
    if(CaseNumberForJoin(columnname2))
    {
        if(tablename2=="NONE")
        {
            comparision3=atoi(columnname2.c_str());
            //cout << "comparision3 " << comparision3 << endl;
            tempColStatus = 0;
        }
        else
        {
            printf("error\n");exit(0);
            fault_status=1;
            return result_row;  
        }
    }
    else if(tablename2=="NONE")
    {
        final_table2=columnname2;
    }
    else
    {
        final_table2=tablename2+"."+columnname2;
    }
    if(tempColStatus == 1)//2nd operator is column case
    {
        // compare values of compare1_index with values of coloumn comparisonOfINdex2
        comparisonOfINdex2=FindColumnsDataFunc(final_table2);
        if(comparisonOfINdex2==-1)
        {
            printf("error\n");exit(0);
            fault_status=1;
            return result_row;
        }
        //cout <<  "comparisonOfINdex2 " << comparisonOfINdex2 << endl;
    }
    // compare values of compare1_index with comparision3/comparisonOfINdex2 values
    if(Constraints_Select[operatorIndex]=="equal")
    {
        for(int i=0;i<RecordsDataofJoinTables.size();i++)
        {
            if(((tempColStatus==0) &&(RecordsDataofJoinTables[i][compare1_index]==comparision3)) || ((tempColStatus==1) &&(RecordsDataofJoinTables[i][compare1_index]==RecordsDataofJoinTables[i][comparisonOfINdex2])))
            {
                result_row.push_back(i);
            }
        }

        // below 4 lines remove redundant data 
        if(tempColStatus==1)
        {
            int var_flag = 0;
            if(SelectAggregateWhereColumns.size()>=2)
            {
                for(int v=0;v<SelectAggregateWhereColumns.size();v++)
                {
                    if(SelectAggregateWhereColumns[v][2]==columnname1)
                        var_flag += 1;
                    else if(SelectAggregateWhereColumns[v][2]==columnname2)
                        var_flag += 1;
                    if(var_flag>=2)
                        break;
                }
            }
            if(var_flag<2)
            {
                for(int i=0;i<RecordsDataofJoinTables.size();i++)
                {
                    RecordsDataofJoinTables[i].erase(RecordsDataofJoinTables[i].begin()+comparisonOfINdex2);
                }
                CrossProductTablesDefinition.erase(CrossProductTablesDefinition.begin()+comparisonOfINdex2);
            }
        }

    }
    else if(Constraints_Select[operatorIndex]=="not_equal")
    {
        for(int i=0;i<RecordsDataofJoinTables.size();i++)
        {
            if(((tempColStatus==0) && (RecordsDataofJoinTables[i][compare1_index]!=comparision3)) || ((tempColStatus==1) &&(RecordsDataofJoinTables[i][compare1_index]!=RecordsDataofJoinTables[i][comparisonOfINdex2])))
            {
                result_row.push_back(i);
            }
        }
    }
    else if(Constraints_Select[operatorIndex]=="greater")
    {
        for(int i=0;i<RecordsDataofJoinTables.size();i++)
        {
            if(((tempColStatus==0) && (RecordsDataofJoinTables[i][compare1_index] > comparision3)) || ((tempColStatus==1) &&(RecordsDataofJoinTables[i][compare1_index] > RecordsDataofJoinTables[i][comparisonOfINdex2])))
            {
                result_row.push_back(i);
            }
        }
    }
    else if(Constraints_Select[operatorIndex]=="greater_equal")
    {
        for(int i=0;i<RecordsDataofJoinTables.size();i++)
        {
            if(((tempColStatus==0) && (RecordsDataofJoinTables[i][compare1_index] >= comparision3)) || ((tempColStatus==1) &&(RecordsDataofJoinTables[i][compare1_index] >= RecordsDataofJoinTables[i][comparisonOfINdex2])))
            {
                result_row.push_back(i);
            }
        }
    }
    else if(Constraints_Select[operatorIndex]=="less_equal")
    {
        for(int i=0;i<RecordsDataofJoinTables.size();i++)
        {
            if(((tempColStatus==0) && (RecordsDataofJoinTables[i][compare1_index] <= comparision3)) || ((tempColStatus==1) &&(RecordsDataofJoinTables[i][compare1_index] <= RecordsDataofJoinTables[i][comparisonOfINdex2])))
            {
                result_row.push_back(i);
            }
        }
    }
    else if(Constraints_Select[operatorIndex]=="lesser")
    {
        for(int i=0;i<RecordsDataofJoinTables.size();i++)
        {
            if(((tempColStatus==0) && (RecordsDataofJoinTables[i][compare1_index] < comparision3)) || ((tempColStatus==1) &&(RecordsDataofJoinTables[i][compare1_index] < RecordsDataofJoinTables[i][comparisonOfINdex2])))
            {
                result_row.push_back(i);
            }
        }
    }
    else
    {
        //cout << "wrong operator" << endl;
        printf("error\n");exit(0);
        fault_status=1;
        return result_row;
    }
    return result_row;
}


void FindSelectColdatafunc()
{
        int i=0,j=0,l=0;
        string tempname,temp1,temp2;
        vector<string> ColumnName;
        vector<int> storeData;
        string temp_name1,temp_name2;
        int op=0,flag1=0;
        
        /*for(i=0;i<SelectAggregateWhereColumns.size();i++)
        {
            for(j=0;j<SelectAggregateWhereColumns[i].size();j++)
                cout << SelectAggregateWhereColumns[i][j] << " ";
            cout << endl;
        } */
        for(i=0;i<SelectAggregateWhereColumns.size();i++)
        {
                tempname.clear();
                ColumnName=SelectAggregateWhereColumns[i];
                if(ColumnName[1]=="NONE")
                {
                        tempname=ColumnName[2];
                }
                else
                {
                        tempname=ColumnName[1];
                        tempname=tempname+"."+ColumnName[2];
                }
                if(ColumnName[0]=="MAX")
                {
                    l=FindColumnsDataFunc(tempname);
                    tempname=CrossProductTablesDefinition[l];

                    for(j=0;j<ResultantRecords.size();j++)
                    {
                        storeData.push_back(ResultantRecords[j][l]);      
                    }
                    int Maxim=INT_MIN;
                    for(l=0;l<storeData.size();l++)
                    {
                        if(Maxim<storeData[l])
                        {
                            Maxim=storeData[l];
                        }
                    }
                    storeData.clear();
                    storeData.push_back(Maxim);
                    tempname="max("+tempname+")";
                    Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                    storeData.clear();
                }
                else if(ColumnName[0]=="MIN")
                {
                        l=FindColumnsDataFunc(tempname);
                        tempname=CrossProductTablesDefinition[l];
                        for(j=0;j<ResultantRecords.size();j++)
                        {
                                storeData.push_back(ResultantRecords[j][l]);      
                        }
                        int mini=INT_MAX;
                        for(l=0;l<storeData.size();l++)
                        {
                                if(mini>storeData[l])
                                {
                                        mini=storeData[l];
                                }
                        }
                        storeData.clear();
                        storeData.push_back(mini);
                        tempname="min("+tempname+")";
                        Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                        storeData.clear();
                }
                else if(ColumnName[0]=="DISTINCT" || QueryDistinct_status==1)
                {
                        if(QueryColumnDistinct_status==0)
                        {
                                l=FindColumnsDataFunc(tempname);
                                tempname=CrossProductTablesDefinition[l];
                                for(j=0;j<ResultantRecords.size();j++)
                                {
                                        storeData.push_back(ResultantRecords[j][l]);      
                                }
                                Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                                storeData.clear();
                        }
                        else
                        {
                                l=FindColumnsDataFunc(tempname);
                                tempname=CrossProductTablesDefinition[l];
                                for(j=0;j<ResultantRecords.size();j++)
                                {
                                        storeData.push_back(ResultantRecords[j][l]);      
                                }
                                tempname="distinct("+tempname+")";
                                Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                                storeData.clear();
                        }
                }
                else if(ColumnName[0]=="SUM")
                {
                        l=FindColumnsDataFunc(tempname);
                        tempname=CrossProductTablesDefinition[l];
                        for(j=0;j<ResultantRecords.size();j++)
                        {
                                storeData.push_back(ResultantRecords[j][l]);      
                        }
                        int finalOutput=0;
                        for(l=0;l<storeData.size();l++)
                        {
                                finalOutput+=storeData[l];
                        }
                        storeData.clear();
                        storeData.push_back(finalOutput);
                        tempname="sum("+tempname+")";
                        Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                        storeData.clear();
                }
                else if(ColumnName[0]=="AVG")
                {
                        vector<float> storeData1;
                        l=FindColumnsDataFunc(tempname);
                        tempname=CrossProductTablesDefinition[l];
                        for(j=0;j<ResultantRecords.size();j++)
                        {
                                storeData1.push_back(ResultantRecords[j][l]);     
                        }
                        float finalOutput=0;
                        for(l=0;l<storeData1.size();l++)
                        {
                                finalOutput+=storeData1[l];
                        }
                        finalOutput=finalOutput/storeData1.size();
                        storeData1.clear();
                        storeData1.push_back(finalOutput);
                        tempname="avg("+tempname+")";
                        FloatingpointRecordstored.push_back(make_pair(tempname,storeData1));
                        storeData1.clear();
                }
                else if(ColumnName[0]=="NONE")
                {
                        if(ColumnName[2]=="ALL")
                        {
                                for(j=0;j<CrossProductTablesDefinition.size();j++)
                                {
                                        tempname=CrossProductTablesDefinition[j];
                                        for(l=0;l<ResultantRecords.size();l++)
                                        {
                                                storeData.push_back(ResultantRecords[l][j]);      
                                        }
                                        Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                                        storeData.clear();
                                }
                        }
                        else
                        {
                                l=FindColumnsDataFunc(tempname);
                                tempname=CrossProductTablesDefinition[l];
                                for(j=0;j<ResultantRecords.size();j++)
                                {
                                        storeData.push_back(ResultantRecords[j][l]);      
                                }
                                Sql_query_resultstored.push_back(make_pair(tempname,storeData));
                                storeData.clear();
                        }
                }
        }
        if(!FloatingpointRecordstored.empty())
        {
                vector<pair<string,vector<float> > >::iterator myit1=FloatingpointRecordstored.begin();
                int pointr=0;
                myit1=FloatingpointRecordstored.begin();
                for(i=0;i<(FloatingpointRecordstored.size()-1);i++)
                {
                        cout<<FloatingpointRecordstored[i].first<<",";
                }
                cout<<FloatingpointRecordstored[i].first<<"\n";
                if(!FloatingpointRecordstored.empty())
                {
                        for(pointr=0;pointr<FloatingpointRecordstored[0].second.size();pointr++)
                        {
                                for(j=0;j<(FloatingpointRecordstored.size()-1);j++)
                                {
                                        printf("%.4f,",(FloatingpointRecordstored[j].second)[pointr]);
                                }
                                printf("%.4f\n",(FloatingpointRecordstored[j].second)[pointr]);
                        }
                }
        }
        else if(!Sql_query_resultstored.empty())
        {
                vector<pair<string,vector<int> > >::iterator myit1=Sql_query_resultstored.begin();
                int pointr=0;
                myit1=Sql_query_resultstored.begin();
                for(i=0;i<(Sql_query_resultstored.size()-1);i++)
                {
                        cout<<Sql_query_resultstored[i].first<<",";
                }
                cout<<Sql_query_resultstored[i].first<<"\n";
                set<vector<int> > hashmap1;
                vector<int> veck;
                //      return;
                if(!Sql_query_resultstored.empty() && QueryDistinct_status==0)
                {
                        for(pointr=0;pointr<Sql_query_resultstored[0].second.size();pointr++)
                        {
                                for(j=0;j<(Sql_query_resultstored.size()-1);j++)
                                {
                                        cout<<(Sql_query_resultstored[j].second)[pointr]<<",";
                                }
                                cout<<(Sql_query_resultstored[j].second)[pointr]<<"\n";
                        }
                }
                else if(!Sql_query_resultstored.empty() && QueryDistinct_status==1)
                {
                        for(pointr=0;pointr<Sql_query_resultstored[0].second.size();pointr++)
                        {
                                veck.clear();
                                for(j=0;j<(Sql_query_resultstored.size());j++)
                                {
                                        veck.push_back((Sql_query_resultstored[j].second)[pointr]);
                                }
                                hashmap1.insert(veck);
                        }
                        set<vector<int> >::iterator newiterator;
                        newiterator=hashmap1.begin();
                        while(newiterator!=hashmap1.end())
                        {
                                for(i=0;i<(*newiterator).size()-1;i++)
                                {
                                        cout<<(*newiterator)[i]<<",";
                                }
                                cout<<(*newiterator)[i]<<"\n";
                                newiterator++;
                        }
                }
        }
}



void FindSelectedDatafunc()
{
    int tempstatus1=0;
    for(int i=0;i<Constraints_Select.size();i++)
    {
        if(Constraints_Select[i]!="NONE")
        {
            tempstatus1=1;
        }
    }
    if(tempstatus1==0)//no where clause
    {
        ResultantRecords=RecordsDataofJoinTables;
        FindSelectColdatafunc();
    }
    else
    {
        vector<int> result_row;
        string cond1;
        int and_condition=-1,or_condition=-1;
        cond1 = Constraints_Select[5]; //AND/OR/NONE
        if(cond1=="AND")
        {
            vector<int> result_row1 = select_rows(1);
            if((fault_status==1) && (result_row1.empty()))
            {
                fault_status = 1;
                printf("error\n");exit(0);
                return;
            }
            //cout << result_row1.size() << endl;
            vector<int> result_row2 = select_rows(2);
            if((fault_status==1) && (result_row2.empty()))
            {
                fault_status = 1;
                printf("error\n");exit(0);
                return;
            }
            int temp_stat=0;
            for(int i=0;i<result_row1.size();i++)
            {
                temp_stat=0;
                for(int j=0;j<result_row2.size();j++)
                {
                    if(result_row1[i]==result_row2[j])
                    {
                        temp_stat=1;
                        break;
                    }
                }
                if(temp_stat==1)
                {
                    result_row.push_back(result_row1[i]);
                }
            }
        }
        else if(cond1=="OR")
        {
            vector<int> result_row1 = select_rows(1);
            if((fault_status==1) && (result_row1.empty()))
            {
                fault_status = 1;
                printf("error\n");exit(0);
                return;
            }
            vector<int> result_row2 = select_rows(2);
            if((fault_status==1) && (result_row2.empty()))
            {
                fault_status = 1;
                printf("error\n");exit(0);
                return;
            }
            set<int> temp_set;
            for(int i=0;i<result_row1.size();i++)
            {
                temp_set.insert(result_row1[i]);
            }
            for(int i=0;i<result_row2.size();i++)
            {
                temp_set.insert(result_row2[i]);
            }
            set<int>::iterator it1=temp_set.begin();
            while(it1!=temp_set.end())
            {
                result_row.push_back((*it1));
                it1++;
            }
        }
        else
        {
            result_row = select_rows(1);
            if((fault_status==1) && (result_row.empty()))
            {
                fault_status = 1;
                printf("error\n");exit(0);
                return;
            }
        }
        // RecordsDataofJoinTables ResultantRecords result_row
        for(int i=0;i<result_row.size();i++)
        {
            ResultantRecords.push_back(RecordsDataofJoinTables[result_row[i]]);
        }
        /*for(int i=0;i<ResultantRecords.size();i++)
        {
            for(int j=0;j<ResultantRecords[i].size();j++)
            {
                printf("%d ",ResultantRecords[i][j]);
            }
            printf("\n");
        }*/
        FindSelectColdatafunc();
    }
}
<%-- 
    Document   : MergeRecords
    Created on : Oct 6, 2018, 12:00:30 AM
    Author     : Amit Verma <amit.verma@orchestranetworks.com>
--%>

<%@page import="com.orchestranetworks.instance.BranchKey"%>
<%@page import="com.orchestranetworks.instance.Repository"%>
<%@page import="com.orchestranetworks.schema.Path"%>

<%@ page import="com.orchestranetworks.service.*" %>
<%@ page import="com.orchestranetworks.addon.daqa.*" %>
<%@ page import="com.onwbp.adaptation.*" %>
<%@ page import="com.orchestranetworks.schema.*" %>
<%@ page import="com.orchestranetworks.mdm.solr.repository.*" %>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<%
    final ServiceContext context = ServiceContext.getServiceContext(request);
    Repository repo = Repository.getDefault();
    Session sess = context.getSession();
    
    AdaptationHome dataSpaceName = repo.lookupHome(BranchKey.forBranchName("EBXSOLRRelational"));
    ProgrammaticService svc = ProgrammaticService.createForSession(sess, dataSpaceName);
    Adaptation adpt = context.getCurrentAdaptation();
    SchemaNode[] nodes = adpt.getSchemaNode().getNodeChildren();
    
    AdaptationTable adptTable = RepositoryUtils.getTable("EBXSOLRRelational", "EBXSOLRRelational", "/root/PROPERTY_CARD");
    Adaptation adpt1 = adptTable.lookupAdaptationByPrimaryKey(PrimaryKey.parseString("1"));
    out.print((String)adpt1.getValueWithoutResolution(Path.parse("./DaqaMetaData/State")));
    
    Procedure proc = new Procedure(){
    
         public void execute(ProcedureContext pc) throws Exception{
              /*Change the match state*/
            AdaptationTable adptTable = RepositoryUtils.getTable("EBXSOLRRelational", "EBXSOLRRelational", "/root/PROPERTY_CARD");
            Adaptation adpt1 = adptTable.lookupAdaptationByPrimaryKey(PrimaryKey.parseString("1"));
            Adaptation adpt2 = adptTable.lookupAdaptationByPrimaryKey(PrimaryKey.parseString("2"));
            ValueContextForUpdate valAdpt1 = pc.getContext(adpt1.getAdaptationName());
            ValueContextForUpdate valAdpt2 = pc.getContext(adpt2.getAdaptationName());
            
            //valAdpt1.setValue("TT", Path.parse("./LEGAL_BLOCK_SUFX"));
            //valAdpt2.setValue("VV", Path.parse("./LEGAL_BLOCK_SUFX"));
            
            valAdpt1.setValue("Pivot", Path.parse("./DaqaMetaData/State"));
            //valAdpt2.setValue("Suspect", Path.parse("./DaqaMetaData/State"));
                
               pc.doModifyContent(adpt1, valAdpt1);
               pc.doModifyContent(adpt2, valAdpt2);
                /*Adaptation newAdpt = pc.doModifyContent(adpt1, valAdpt1);
                if(newAdpt == null){
                    out.print("This is null");
                }*/
                //out.print((String)newAdpt.get(Path.parse("./DaqaMetaData/State")));
                //pc.doModifyContent(adpt2, valAdpt2);
                
                 MatchingOperations operations = MatchingOperationsFactory.getMatchingOperations();
                  final RecordContext rcMatch = new RecordContext(adpt1, pc);
                  final RecordContext rcCandidate = new RecordContext(adpt2, pc);
                 int cluster =       operations.addIntoCluster(rcMatch);
                operations.addIntoCluster(rcCandidate, cluster);
                operations.automaticMerge(rcCandidate);
         }
    };
    svc.execute(proc);
    
    Procedure procedure = new Procedure(){
            public void execute(ProcedureContext pc) throws Exception{
                AdaptationTable adptTable = RepositoryUtils.getTable("EBXSOLRRelational", "EBXSOLRRelational", "/root/PROPERTY_CARD");
                
                Adaptation adpt1 = adptTable.lookupAdaptationByPrimaryKey(PrimaryKey.parseString("1"));
                Adaptation adpt2 = adptTable.lookupAdaptationByPrimaryKey(PrimaryKey.parseString("2"));
                
                MatchingOperations operations = MatchingOperationsFactory.getMatchingOperations();
                        
                final RecordContext rcMatch = new RecordContext(adpt2, pc);
                final RecordContext rcCandidate = new RecordContext(adpt1, pc);
                       
                operations.automaticMerge(rcMatch);
                operations.alignForeignKeys(rcMatch);
                
            }
        };

      // svc.execute(procedure);
    
%>
</html>

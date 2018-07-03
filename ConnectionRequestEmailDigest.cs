using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;

using Quartz;

using Rock;
using Rock.Attribute;
using Rock.Communication;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;


namespace org.ourcitychurch.Jobs
{
	[GroupField("Connection Group","Members of this group will be emailed",true, null ,null,1)]
	[SystemEmailField("System Email", "The system email to use when sending reminder.", true, "a1911882-19dd-4197-a8d8-63cbd8a7d80b", null, 2)]
	[DisallowConcurrentExecution]
	class ConnectionRequestEmailDigest : IJob
	{
		public virtual void Execute(IJobExecutionContext context)
		{
			JobDataMap dataMap = context.JobDetail.JobDataMap;
			var rockContext = new RockContext();
			var groupService = new GroupService(rockContext);
			var groupMemberService = new GroupMemberService(rockContext);
			var connectionRequestService = new ConnectionRequestService(rockContext);

			var group = groupService.GetByGuid(dataMap.GetString("ConnectionGroup").AsGuid());
			int ConnectionRemindersSent = 0;

			if (group != null)
			{
				

				var childrenGroups = groupService.GetAllDescendents(group.Id);

				var allGroups = childrenGroups.ToList();

				allGroups.Add(group);

				foreach (var g in allGroups)
				{
					var connectors = groupMemberService.GetByGroupId(g.Id);

					foreach (var connector in connectors)
					{
						var connectionRequests = connectionRequestService.Queryable().Where(x => x.ConnectorPersonAliasId == connector.Person.PrimaryAliasId && !(x.ConnectionState == ConnectionState.Connected || x.ConnectionState == ConnectionState.Inactive || (x.ConnectionState == ConnectionState.FutureFollowUp && x.FollowupDate > DateTime.Today))).OrderBy(x => x.ConnectionOpportunityId).ToList();

						if (connectionRequests.Count > 0)
						{

							var mergeObjects = Rock.Lava.LavaHelper.GetCommonMergeFields(null, connector.Person);
							mergeObjects.Add("Person", connector.Person);
							mergeObjects.Add("Requests", connectionRequests);

							var recipients = new List<RecipientData>();
							recipients.Add(new RecipientData(connector.Person.Email, mergeObjects));

							var emailMessage = new RockEmailMessage(dataMap.GetString("SystemEmail").AsGuid());
							emailMessage.SetRecipients(recipients);
							emailMessage.Send();

							ConnectionRemindersSent++;

						}

					}

				}



			}

			context.Result = string.Format("{0} Connection reminders sent", ConnectionRemindersSent);

		}
	}
}

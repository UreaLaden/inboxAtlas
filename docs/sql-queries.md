## Messages from the last 30 Days
```sql
SELECT
    m.received_at,
    m.from_email,
    m.domain,
    m.subject,
    COALESCE(lc.display_name, je.value) AS label,
    COALESCE(mc.category, 'unclassified') AS category
  FROM messages m
  JOIN json_each(m.labels) je
  LEFT JOIN label_catalog lc
    ON lc.mailbox_id = m.mailbox_id AND lc.label_id = je.value
  LEFT JOIN message_classifications mc
    ON mc.message_id = m.id AND mc.mailbox_id = m.mailbox_id
  WHERE m.mailbox_id = (SELECT id FROM mailboxes WHERE alias = 'acr')
    AND m.received_at >= '2026-04-08T00:00:00Z'
  ORDER BY m.received_at DESC;
  ```

## Unclassified messages since 30 Days
  ```sql
    SELECT
    m.domain,
    m.from_email,
    COUNT(DISTINCT m.id) AS message_count
  FROM messages m
  LEFT JOIN message_classifications mc
    ON mc.message_id = m.id AND mc.mailbox_id = m.mailbox_id
  WHERE m.mailbox_id = (SELECT id FROM mailboxes WHERE alias = 'acr')
    AND m.received_at >= '2026-04-08T00:00:00Z'
    AND (mc.category IS NULL OR mc.category = 'unknown')
  GROUP BY m.domain, m.from_email
  ORDER BY message_count DESC;
  ```

## Classification Gap Output
  ```sql
  SELECT
  m.domain,
  m.from_email,
  COUNT(*) AS message_count,
  MAX(m.received_at) AS latest,
  GROUP_CONCAT(DISTINCT SUBSTR(m.subject,1,60)) AS sample_subjects
FROM messages m
WHERE m.mailbox_id = (SELECT id FROM mailboxes WHERE alias = 'acr')
  AND (
    m.domain IN ('advisorycloud.com','akerman.com','alcobalaw.com','allpartnerscore.co','am.atlassian.com','angelsnexthq.co','aol.com','apintego.com','belaysolutions.com','bellsouth.net','brex.com','cambridgefp.com','captiveseasaquariums.com','cdresi.com','cityofbocaraton.ccsend.com','clean.email','collab.social','comms.waveapps.com','coolpoolsbygerry.net','curanthealth.com','debraangilletta.com','doublehq.com','e.atlassian.com','e.linkedin.com','ealingbuilds.com','editorsfaves.com','efleets.com','email.accountingweb.com','email.wisestamp.com','emailnotify.net','exlservice.com','financialservicing.net','finsync.com','flexrentalsolutions.com','frontporchds.com','fundbox.com','global.metamail.com','gmail.com','googlemail.com','goteamhutch.com','heat.com','hello.dext.com','homebotapp.com','hondafinancialservices.com','houzz.com','info17.citi.com','inform.progress.com','integrusfirm.com','itsdelish.com','jassolopez.com','journeytobusinesssuccess.com','keeper.app','keydesignmedia.com','kjbm.qbpowerhour.com','libertybenefitsllc.com','lifeinartpics.com','lyft.com','lyonsandsmith.com','m.efile4biz.com','mail.taxprotectionplus.com','mail.thehartford.com','mcnabexec.com','mdscripts.com','me.com','mendelsonconsulting.ccsend.com','mg.salestaxsolutions.us','middleriverah.com','mycbs.ca','mycoreinsurance.com','newtekone.com','oakstreetfunding.com','odysseyelixir.com','officefurnitureonline.ccsend.com','oracle.com','passperfect.com','piano.hellosimply.com','plative.com','powerslaw.com','practicepanda.com','principalfeedback.com','products.progress.com','progress.com','promomail.microsoft.com','quora.com','ramp.com','savings.lendingtree.com','sg.booking.com','standardcnst.com','structuretitle.com','thebfis.com','thehartford.com','thepropertysolutionsguru.com','tjm-law.com','tmtproperties.com','trcgconsulting.com','trust2change.com','ubs.com','ultimatebundlesmedia.com','updates.bizequity.com','usastacknoble.co','windowslive.com','wmmc11.greendot.com','yahoo.com','zixmessagecenter.com','zulo.ccsend.com')
    OR m.from_email IN ('aaron.fant@ubs.com','accountingpartner@finsync.com','advertise-noreply@global.metamail.com','alwayscitizen@me.com','andrea@journeytobusinesssuccess.com','andy@keeper.app','automated@apintego.com','azure@promomail.microsoft.com','bcarson@mcnabexec.com','bcollins@officefurnitureonline.ccsend.com','bestbuycard@info17.citi.com','billing@practicepanda.com','bootcamp@mycbs.ca','brenda@standardcnst.com','brinni@structuretitle.com','brittany.goins@thehartford.com','c.mitchell@allpartnerscore.co','chris_beaver@advisorycloud.com','cliffordknightsii@gmail.com','conrad.coke@cambridgefp.com','darrenhayesga@gmail.com','davejfogg@gmail.com','david.ritmo@akerman.com','debra@debraangilletta.com','do_not_reply@hondafinancialservices.com','do-not-reply@efleets.com','drp@middleriverah.com','economicdevelopment@cityofbocaraton.ccsend.com','editor@email.accountingweb.com','elisabeth.schaser@frontporchds.com','email.campaign@sg.booking.com','email@savings.lendingtree.com','english-personalized-digest@quora.com','erica.mckenney@powerslaw.com','erik@jassolopez.com','event@hello.dext.com','floridablue.notification@zixmessagecenter.com','followups@clean.email','gbdcustomerservice@thehartford.com','gerry@coolpoolsbygerry.net','greendot@wmmc11.greendot.com','hello@editorsfaves.com','hello@homebotapp.com','hello@plative.com','hondafinancialservices@emailnotify.net','info@belaysolutions.com','info@cdresi.com','info@e.atlassian.com','info@email.wisestamp.com','info@flexrentalsolutions.com','info@itsdelish.com','info@keydesignmedia.com','info@lifeinartpics.com','info@m.efile4biz.com','info@mg.salestaxsolutions.us','info@trust2change.com','insurance@mail.taxprotectionplus.com','jack@doublehq.com','jalcoba@alcobalaw.com','jamie@thepropertysolutionsguru.com','jay.barron@usastacknoble.co','jenf@goteamhutch.com','jenifer.m.buehler@efleets.com','jessica@ultimatebundlesmedia.com','jezrahlee@financialservicing.net','john.j.hughes.iv@gmail.com','johnkylemanuel@financialservicing.net','josalexander30@gmail.com','josh.aronin@odysseyelixir.com','jpetford@ealingbuilds.com','julie@libertybenefitsllc.com','kettelyauguste@gmail.com','lance@captiveseasaquariums.com','linkedin@e.linkedin.com','listen@principalfeedback.com','llew315@bellsouth.net','lostmountainlocksmith@gmail.com','lourdesvalentin89@gmail.com','ltejada@mcnabexec.com','lucy.grossman@heat.com','lyft_ar_no_reply@lyft.com','m_ahmad@windowslive.com','mailer-daemon@googlemail.com','marketing@comms.waveapps.com','marketing@practicepanda.com','marketing@updates.bizequity.com','maryjune.m.navia@financialservicing.net','meagan.donnelly@oracle.com','menicholsinc@bellsouth.net','mfossati@mycoreinsurance.com','mitchell@zulo.ccsend.com','monitor@mdscripts.com','mtracey@curanthealth.com','news@kjbm.qbpowerhour.com','nlmnyc@aol.com','no_reply@am.atlassian.com','npetrak@brex.com','osf@oakstreetfunding.com','patricia@trcgconsulting.com','pcitizen2@gmail.com','play@piano.hellosimply.com','plus@fundbox.com','ppmarketing@passperfect.com','progress@inform.progress.com','progress@products.progress.com','qb@mendelsonconsulting.ccsend.com','ruby.preston@angelsnexthq.co','sabresh.mouli@progress.com','sales@mail.taxprotectionplus.com','savannahyoga@gmail.com','shelby@collab.social','slyons@lyonsandsmith.com','support@fundbox.com','support@mdscripts.com','support@practicepanda.com','support@ramp.com','sydaviat@gmail.com','tatiannaknights@yahoo.com','thomas.mazzarisi@tjm-law.com','update@global.metamail.com','updates@houzz.com','vernon@thebfis.com','victoria.belke@exlservice.com','vixamarsteve@gmail.com','willardshepardlaw@gmail.com')
  )
GROUP BY m.domain, m.from_email
ORDER BY message_count DESC, m.domain ASC;

  ```
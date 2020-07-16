* Issues *
Here we can make a list of things that we are either:

* Tracking incorrectly
* Tracking in a different way across websites
* Are not tracking at all

Last update 16/10/2018:
- 'Burger menu' is being tracked as 'Service Bar' in all storefronts except from VILA
- Category click (on the plp view) has different names in VILA (compared wit all the other storefronts where the label is 'Filter')
- Loading times new metric is only being tracked for main storefronts
- Session and client ID new metrics (customDimensions.index) are only being tracked for BS.com
- A 'click' event in service bar is tracked in BS.com (every click) but on the other storefronts only the last click is considered event
- A click in banner (home page) most of the times generates 2 events (tested for VM account) --> one of them has a NULL label (check banner clicks query)

Last update 11/02/2019:
- The event for the appearance of Notify Me after clicking on a out-of-stock size (Category: Notify_me, Action: Visible) is only track on ONLY.DE, but not on VEROMODA.DE.
- session_code is unique per session, but beware when counting per date/storefront, as a session can bridge between storefronts or dates (midnight) without session_code being reset.
- is_Exit is not unique per session, but can be repeated if the visitor closes the browser multiple times in the same session. For last action committed use max(hits.hit_number).
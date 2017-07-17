import {
    createTestStore,
    login,
} from "metabase/__support__/integrated_tests";
import { refreshSiteSettings } from "metabase/redux/settings";
import { initializeSettings } from "metabase/admin/settings/settings";
import { mount } from "enzyme";
import { loadCurrentUser } from "metabase/redux/user";
import { initializeMetadata } from "metabase/admin/datamodel/datamodel";

export const createWaitForElement = (rootComponent, maxTime = 2000, interval = 10) => selector => {
    return new Promise((resolve, reject) => {
        let remainingTime = maxTime;

        const intervalId = setInterval(() => {
            if (remainingTime < 0) {
                clearInterval(intervalId);
                return reject(new Error(`Expected to find ${selector} within ${maxTime}ms, but it was never found.`))
            }

            const targetComponent = rootComponent.find(selector);
            if (targetComponent.length) {
                clearInterval(intervalId);
                return resolve(targetComponent);
            }

            remainingTime = remainingTime - interval;
        }, interval)
    });
};

function timeout(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

describe("admin/settings", () => {
    beforeAll(async () => {
        await login();
    })

    describe("admin settings", () => {
        it("should persist a setting", async () => {
            const store = await createTestStore();
            await store.dispatch(refreshSiteSettings());
            // // do all needed metadata loading before the page push
            await initializeSettings();
            await loadCurrentUser();

            // pick a random site name to try updating it to
            const siteName = "Metabase" + Math.random();

            // load the "general" pane of the admin settings
            store.pushPath('/admin/settings/general');

            // first just make sure the site name isn't already set (it shouldn't since we're using a random name)
            const appWrapper = mount(store.getAppContainer())
            const waitFor = createWaitForElement(appWrapper);

            const input = appWrapper.find(".SettingsInput").first()

            expect(input.prop("value")).not.toBe(siteName);

            // update the field value, then blur to trigger the update
            input.simulate('change', { target: { value: siteName } })
            input.simulate('blur')

            // wait for the loading indicator to show success
            // TODO: Is this the best way to "wait" in Enzyme tests for our needs?
            await waitFor(".SaveStatus.text-success");

            // see if the value has actually changed by changing the page and returning to settings page
            store.pushPath("/");
            store.pushPath("/admin/settings/general");

            // verify the site name value was persisted
            expect(appWrapper.find(".SettingsInput").first().prop("value")).toBe(siteName);
        });
    });

    describe("data model editor", () => {
        it("should allow admin to edit data model", async () => {
            const store = await createTestStore();
            store.pushPath('/admin/datamodel/database');
            await initializeMetadata();
            const appWrapper = mount(store.getAppContainer())

            const waitFor = createWaitForElement(appWrapper);
            const adminListItems = (await waitFor(".AdminList-item")).children()
            adminListItems.at(1).simulate("click");

            // unhide
            const visibilityTypes = (await waitFor("#VisibilityTypes")).children()
            visibilityTypes.at(1).simulate("click");

            // const visibilitySubTypes = visibilityTypes.find("#VisibilitySubTypes").children();
            // visibilitySubTypes.at(1).simulate("click");

            // hide fields from people table
            // adminListItems.at(2).simulate("click");

            // Requires still extra work:

            // const columnsListItems = appWrapper.find("#ColumnsList").find("li")
            // columnsListItems.at(0).find(".TableEditor-field-visibility").simulate("click");

            // const columnarSelectorRows = (await waitFor(".ColumnarSelector-rows")).find("li")
            // console.log(columnarSelectorRows.at(1).debug())

            // columnarSelectorRows.at(1).find(".ColumnarSelector-row").simulate("click");
            // await waitForElementAndClick(".ColumnarSelector-rows li:nth-child(2) .ColumnarSelector-row");
            // columnsListItems.find("li").at(1).find(".TableEditor-field-visibility").simulate("click");
            // await waitForElementAndClick("#ColumnsList li:nth-child(2) .TableEditor-field-visibility");
            // await waitForElementAndClick(".ColumnarSelector-rows li:nth-child(3) .ColumnarSelector-row");
            //
            // // modify special type for address field
            // await waitForElementAndClick("#ColumnsList li:first-child .TableEditor-field-special-type");
            // await waitForElementAndClick(".ColumnarSelector-rows li:nth-child(2) .ColumnarSelector-row");

            //TODO: verify tables and fields are hidden in query builder
        });
    });
});

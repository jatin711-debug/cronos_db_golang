import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { it, expect } from "vitest";
import { ErrorAlert } from "./ErrorAlert";

it("explains missing admin permission without requesting another login", () => {
  render(<MemoryRouter><ErrorAlert message="403 subject is not an admin" /></MemoryRouter>);
  expect(screen.getByText("Admin access required")).toBeInTheDocument();
  expect(screen.queryByText("Authentication required")).not.toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Sign in" })).not.toBeInTheDocument();
});

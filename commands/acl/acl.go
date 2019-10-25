package acl

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Perform acl related actions",
		Args:  cobra.ExactArgs(0),
	}

	cmd.AddCommand(
		createComand(cl),
		describeCommand(cl),
		deleteCommand(cl),
	)

	return cmd
}
